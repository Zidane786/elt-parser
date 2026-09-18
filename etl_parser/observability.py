"""Run-scoped structured events and bounded metrics, independent of any provider SDK.

Implements the observability contract described in
``docs/superpowers/specs/2026-09-17-ai-lineage-observability-github-design.md``:
:class:`RunObserver` is a thread-safe event/metric sink for one deterministic or
AI-assisted run, created and looked up through :func:`current_observer` and the
:func:`observed` decorator rather than through global logging configuration.
Console events are always emitted to stderr; when ``log_dir`` is given, the same
events are additionally persisted as rotated ``events*.jsonl`` files, with
``metrics.json`` and ``manifest.json`` written on :meth:`RunObserver.finish`. All
emitted values pass through :func:`sanitize`, which redacts known-sensitive keys and
credential-shaped strings; raw source text, prompts, responses and exception
messages/stacks are deliberately never logged.
"""

from __future__ import annotations

import asyncio
import contextvars
import hashlib
import inspect
import json
import logging
import math
import os
import re
import sys
import tempfile
import threading
import time
from collections import Counter, deque
from collections.abc import Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from uuid import uuid4

_CURRENT = contextvars.ContextVar("etl_parser_observer", default=None)
_SPAN = contextvars.ContextVar("etl_parser_span", default=None)
_SENSITIVE = {
    "authorization",
    "password",
    "passwd",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "api_key",
    "anthropic_api_key",
    "extra_headers",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "github_token",
    "gh_token",
    "cookie",
    "set-cookie",
    "headers",
    "source_text",
    "prompt",
    "response",
    "body",
    "expression",
    "bindings",
    "traceback",
    "exception_message",
}
_CREDENTIALS = re.compile(r"(\b[a-z][a-z0-9+.-]*://)[^\s/@]+:[^\s/@]+@", re.I)
_AUTH = re.compile(r"(?i)\b(bearer|basic)\s+[^\s,;]+")
_KEY_VALUE = re.compile(
    r"(?i)((?:password|secret|token|api[_-]?key|access[_-]?key)[\w-]*\s*[=:]\s*)[^\s&,;]+"
)


def digest(value: str) -> str:
    """Compute a hex SHA-256 digest of a string.

    Used to fingerprint source text and content for logs and manifests without
    persisting the underlying text itself.

    Args:
        value: The text to hash.

    Returns:
        str: The hex-encoded SHA-256 digest of ``value``.
    """
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sanitize(value, *, secrets=(), depth=0):
    """Recursively redact sensitive keys, credential-shaped strings and oversize data.

    Best-effort metadata redaction for values headed to an event/log/metrics sink; it
    is not permission to log arbitrary source text, prompts or responses, which
    callers must keep out of the input entirely. Mapping keys matching a known
    sensitive-field name are replaced wholesale; string values have embedded
    credentials, ``Authorization``-style headers and ``key=value`` secrets masked and
    are truncated to 2000 characters; lists/tuples/sets are capped at 100 items; and
    recursion is capped at a depth of 8.

    Args:
        value: The value to sanitize (mapping, list/tuple/set, string, number, or
            other JSON-adjacent type).
        secrets: Additional literal secret strings to redact wherever they appear in
            string values, beyond the built-in credential/token patterns.
        depth: Current recursion depth; internal use for the depth limit.

    Returns:
        A structurally equivalent value with sensitive keys replaced by
        ``"[redacted]"``, sensitive substrings masked, oversize collections
        truncated, non-finite floats replaced with ``None``, and any otherwise
        unsupported type replaced by a ``"[TypeName]"`` placeholder.
    """
    if depth > 8:
        return "[depth-limit]"
    if isinstance(value, Mapping):
        return {
            str(key): "[redacted]"
            if str(key).lower() in _SENSITIVE
            else sanitize(item, secrets=secrets, depth=depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        result = [sanitize(item, secrets=secrets, depth=depth + 1) for item in list(items)[:100]]
        if len(items) > 100:
            result.append({"omitted_items": len(items) - 100})
        return result
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[redacted]")
        value = _CREDENTIALS.sub(r"\1[redacted]@", value)
        value = _AUTH.sub(r"\1 [redacted]", value)
        value = _KEY_VALUE.sub(r"\1[redacted]", value)
        return value[:2000] + ("[truncated]" if len(value) > 2000 else "")
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return f"[{type(value).__name__}]"


class RunObserver:
    """Thread-safe streaming event sink and metrics recorder for a single run.

    Emits sanitized JSON event records to stderr via an isolated ``Logger`` (never the
    root logger) and, when ``log_dir`` is given, to rotated ``events*.jsonl`` files
    under a per-run directory. Counters, gauges and duration histograms are kept in
    bounded memory (recent duration samples are capped by a fixed-size deque) rather
    than as unbounded event history, and are written out as ``metrics.json`` and
    ``manifest.json`` by :meth:`finish`.

    Attributes:
        run_id: Unique hex identifier for this run.
        started_at: ISO-8601 UTC timestamp when the run started.
        run_dir: Directory holding this run's persisted log/summary files, or
            ``None`` when only console logging is enabled.
        counters: Cumulative named counters.
        gauges: Latest named gauge values.
        configuration: Sanitized configuration values recorded via :meth:`configure`.
        durations: Per-stage duration statistics recorded via :meth:`duration`.
        status: Overall run status: ``"success"``, ``"partial"``, ``"failed"`` or
            ``"cancelled"``.
        sink_failed: Whether persisting events to disk has failed for this run.
        logger: The isolated, non-propagating ``logging.Logger`` used for console
            output.
        max_log_bytes: Maximum size in bytes of a single ``events*.jsonl`` file
            before rotating to the next one.
        max_log_files: Maximum number of rotated event files before logging stops
            persisting to disk.
    """

    def __init__(
        self,
        *,
        log_dir=None,
        log_level="INFO",
        max_log_bytes=10_000_000,
        max_log_files=20,
        secrets=(),
    ):
        """Create a run observer, optionally persisting events under ``log_dir``.

        Args:
            log_dir: Parent directory for a per-run log folder (named from a UTC
                timestamp and the run id). When ``None``, only console logging is
                used.
            log_level: Minimum level for console logging: one of ``DEBUG``, ``INFO``,
                ``WARNING``, ``ERROR`` or ``CRITICAL`` (case-insensitive).
            max_log_bytes: Maximum size in bytes of one ``events*.jsonl`` file before
                rotating; must be at least 1024.
            max_log_files: Maximum number of rotated event files; must be at least 1.
            secrets: Literal secret strings to redact from logged values, in addition
                to values from environment variables whose name matches a known
                sensitive key.

        Raises:
            ValueError: If ``log_level`` is not a recognized level, or if
                ``max_log_bytes``/``max_log_files`` are below their minimums.
            OSError: If ``log_dir`` cannot be created or the initial log file cannot
                be opened.
        """
        level = str(log_level).upper()
        if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("log_level must be DEBUG, INFO, WARNING, ERROR or CRITICAL")
        if max_log_bytes < 1024 or max_log_files < 1:
            raise ValueError("Log limits require >=1024 bytes and >=1 file")
        self.run_id = uuid4().hex
        self.started_at = datetime.now(UTC).isoformat()
        self.started = time.perf_counter()
        self.run_dir = None
        self.counters = Counter()
        self.gauges = {}
        self.configuration = {}
        self.durations = {}
        self.status = "success"
        self._finished = False
        self._lock = threading.RLock()
        self._sequence = 0
        self._handle = None
        self._file_number = 0
        self._file_bytes = 0
        self.max_log_bytes = max_log_bytes
        self.max_log_files = max_log_files
        self.sink_failed = False
        self._opened_files = 0
        self._secrets = tuple(secrets) + tuple(
            value
            for key, value in os.environ.items()
            if key.lower() in _SENSITIVE and len(value) >= 4
        )
        # An isolated Logger avoids modifying application/root logging configuration.
        self.logger = logging.Logger(f"etl_parser.run.{self.run_id}", getattr(logging, level))
        self.logger.propagate = False
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(message)s"))
        self.logger.addHandler(handler)
        if log_dir is not None:
            folder = Path(log_dir)
            folder.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
            self.run_dir = folder / f"{stamp}_{self.run_id}"
            self.run_dir.mkdir(mode=0o700)
            self._open_log()

    def _open_log(self):
        """Open the next owner-only ``events*.jsonl`` file for this run.

        Raises:
            OSError: If the file already exists or cannot be created (mode ``0o600``).
        """
        name = "events.jsonl" if self._file_number == 0 else f"events.{self._file_number:03}.jsonl"
        fd = os.open(self.run_dir / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self._handle = os.fdopen(fd, "w", encoding="utf-8")
        self._opened_files += 1
        self._file_bytes = 0

    def count(self, name, amount=1):
        """Increment a named counter.

        Args:
            name: Counter name.
            amount: Amount to add; defaults to 1.
        """
        with self._lock:
            self.counters[name] += amount

    def gauge(self, name, value):
        """Set a named gauge to its latest value.

        Args:
            name: Gauge name.
            value: The value to record.
        """
        with self._lock:
            self.gauges[name] = value

    def configure(self, **values):
        """Record sanitized configuration values for this run's manifest/metrics.

        Args:
            **values: Configuration key/value pairs; passed through :func:`sanitize`
                before storage.
        """
        with self._lock:
            self.configuration.update(sanitize(values, secrets=self._secrets))

    def protect(self, *secrets):
        """Register explicitly supplied secrets before emitting provider metadata."""
        with self._lock:
            self._secrets += tuple(value for value in secrets if value)

    def partial(self):
        """Downgrade run status from ``"success"`` to ``"partial"``, if not already set.

        Leaves ``"failed"``/``"cancelled"`` statuses unchanged.
        """
        with self._lock:
            if self.status == "success":
                self.status = "partial"

    def duration(self, name, milliseconds):
        """Record one duration sample for a named stage.

        Args:
            name: Stage/operation name.
            milliseconds: Elapsed time in milliseconds for this sample.
        """
        with self._lock:
            record = self.durations.setdefault(
                name,
                {
                    "count": 0,
                    "total_ms": 0.0,
                    "min_ms": milliseconds,
                    "max_ms": milliseconds,
                    "recent": deque(maxlen=256),
                },
            )
            record["count"] += 1
            record["total_ms"] += milliseconds
            record["min_ms"] = min(record["min_ms"], milliseconds)
            record["max_ms"] = max(record["max_ms"], milliseconds)
            record["recent"].append(milliseconds)

    def event(self, event, *, level="INFO", actor="framework", **fields):
        """Emit one sanitized, sequenced JSON event to the console and, if open, disk.

        Silently does nothing once :meth:`finish` has run. Sensitive fields are
        redacted via :func:`sanitize` before serialization. If writing to the current
        ``events*.jsonl`` file would exceed ``max_log_bytes``, rotates to the next
        file; if the file limit is reached or a write fails, disk persistence is
        disabled for the rest of the run (``sink_failed`` is set and the run is marked
        partial) while console logging continues.

        Args:
            event: Event name/type, e.g. ``"ai.request.started"``.
            level: Log level name: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR`` or
                ``CRITICAL``.
            actor: Who/what produced the event, e.g. ``"framework"``, ``"policy"``,
                ``"agent_sdk"``.
            **fields: Additional structured fields to include, sanitized before
                emission.
        """
        with self._lock:
            if self._finished:
                return
            self._sequence += 1
            record = sanitize(
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "run_id": self.run_id,
                    "event_id": f"{self.run_id}:{self._sequence}",
                    "span_id": _SPAN.get(),
                    "level": level,
                    "actor": actor,
                    "event": event,
                    **fields,
                },
                secrets=self._secrets,
            )
            text = json.dumps(record, sort_keys=True, ensure_ascii=True, allow_nan=False)
            self.counters["events.emitted"] += 1
            self.logger.log(getattr(logging, level), text)
            if self._handle is None:
                if self.sink_failed:
                    self.counters["events.not_persisted"] += 1
                return
            try:
                size = len(text.encode("utf-8")) + 1
                if self._file_bytes and self._file_bytes + size > self.max_log_bytes:
                    self._handle.close()
                    self._handle = None
                    self._file_number += 1
                    if self._file_number >= self.max_log_files:
                        raise OSError("Log file limit reached")
                    self._open_log()
                self._handle.write(text + "\n")
                self._handle.flush()
                self._file_bytes += size
                self.counters["events.persisted"] += 1
            except OSError:
                if self._handle:
                    try:
                        self._handle.close()
                    except OSError:
                        pass
                self._handle = None
                self.sink_failed = True
                self.partial()
                self.counters["logging.failures"] += 1
                self.counters["events.not_persisted"] += 1
                self.logger.error(
                    json.dumps(
                        {
                            "event": "logging.persistence_failed",
                            "run_id": self.run_id,
                            "reason": "io_error_or_log_limit",
                            "message": "Console logging continues; persisted audit is incomplete.",
                        }
                    )
                )

    @contextmanager
    def span(self, stage, **fields):
        """Time and emit start/finish events around a named stage of work.

        Nests spans via a context-local parent span id, records a duration sample for
        ``stage``, and classifies outcome as ``"cancelled"`` (on
        ``KeyboardInterrupt``/``SystemExit``/``asyncio.CancelledError``) or
        ``"failed"`` (any other exception) versus ``"success"``. Any exception raised
        inside the ``with`` block is recorded and then re-raised unchanged.

        Args:
            stage: Name of the stage/operation being timed, e.g. ``"analysis.run"``.
            **fields: Additional structured fields attached to the start/finish (and,
                on failure, failure) events.

        Yields:
            None.

        Raises:
            BaseException: Whatever exception the wrapped block raises, after
                recording it.
        """
        parent = _SPAN.get()
        token = _SPAN.set(uuid4().hex)
        start = time.perf_counter()
        self.event("stage.started", stage=stage, parent_span_id=parent, **fields)
        status = "success"
        try:
            yield
        except BaseException as exc:
            status = (
                "cancelled"
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError))
                else "failed"
            )
            self.count(f"stage.{stage}.{status}")
            self.event(
                "stage.failed", level="ERROR", stage=stage, error_type=type(exc).__name__, **fields
            )
            raise
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            self.duration(stage, elapsed)
            self.event(
                "stage.finished",
                stage=stage,
                status=status,
                duration_ms=round(elapsed, 3),
                **fields,
            )
            _SPAN.reset(token)

    def snapshot(self):
        """Build the current metrics summary (this run's ``metrics.json`` content).

        Includes counters, gauges, per-stage duration statistics (with a p95 over the
        most recent samples), and whole-run throughput. Cost and accuracy are always
        reported as ``None`` with an explanatory note: no pricing model is applied and
        confidence/agreement counts are not independently measured accuracy.

        Returns:
            dict: The sanitized, JSON-serializable metrics snapshot.
        """
        with self._lock:
            elapsed = time.perf_counter() - self.started
            durations = {}
            for name, values in self.durations.items():
                recent = sorted(values["recent"])
                durations[name] = {k: v for k, v in values.items() if k != "recent"} | {
                    "mean_ms": values["total_ms"] / values["count"],
                    "recent_sample_count": len(recent),
                    "recent_p95_ms": recent[math.ceil(len(recent) * 0.95) - 1],
                }
            return sanitize(
                {
                    "version": "1",
                    "run_id": self.run_id,
                    "started_at": self.started_at,
                    "status": self.status,
                    "duration_ms": (time.perf_counter() - self.started) * 1000,
                    "counters": dict(sorted(self.counters.items())),
                    "gauges": dict(sorted(self.gauges.items())),
                    "durations": durations,
                    "throughput": {
                        "files_per_run_second": self.counters.get("files.parsed", 0) / elapsed
                        if elapsed > 0
                        else None,
                        "indexed_bytes_per_run_second": self.gauges.get("source.indexed_bytes", 0)
                        / elapsed
                        if elapsed > 0
                        else None,
                        "note": "Whole-run averages, not isolated parser or model throughput.",
                    },
                    "cost_estimate": None,
                    "cost_note": "Unknown: no pricing configuration is applied.",
                    "accuracy": None,
                    "accuracy_note": "Confidence counts are not independently measured accuracy.",
                },
                secrets=self._secrets,
            )

    def _write_json(self, filename, value):
        """Write a JSON value to ``self.run_dir`` atomically via a temp-file rename.

        Args:
            filename: Destination filename within ``self.run_dir``.
            value: JSON-serializable value to write, pretty-printed with sorted keys.

        Raises:
            OSError: If the temporary file cannot be created, written or renamed.
        """
        fd, path = tempfile.mkstemp(prefix=".summary-", dir=self.run_dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
                stream.write("\n")
            os.replace(path, self.run_dir / filename)
        finally:
            if os.path.exists(path):
                os.unlink(path)

    def finish(self, *, error=None):
        """Finalize the run: emit a closing event and persist metrics/manifest once.

        Idempotent — a second call is a no-op. Sets terminal status from ``error``
        when given (``"cancelled"`` for ``KeyboardInterrupt``/``CancelledError``,
        otherwise ``"failed"``), closes the event log file before writing the
        manifest (so a late flush failure cannot produce a manifest that claims a
        complete persisted audit), then writes ``metrics.json`` and
        ``manifest.json`` under ``run_dir`` if logging to disk is enabled. Errors
        while writing those summary files mark the run ``"failed"`` and
        ``sink_failed`` but do not raise. Always detaches and closes the console log
        handler.

        Args:
            error: The exception that ended the run, if any, used to classify the
                final status; ``None`` for a normal completion.
        """
        with self._lock:
            if self._finished:
                return
            if error is not None:
                self.status = (
                    "cancelled"
                    if isinstance(error, (KeyboardInterrupt, asyncio.CancelledError))
                    else "failed"
                )
            self.event(
                "run.finished",
                status=self.status,
                error_type=(type(error).__name__ if error is not None else None),
                counters=dict(self.counters),
                duration_ms=(time.perf_counter() - self.started) * 1000,
            )
            # Close before recording audit_complete: delayed flush errors must not
            # leave a manifest claiming a complete persisted audit.
            if self._handle:
                try:
                    self._handle.close()
                except OSError:
                    self.sink_failed = True
                    self.status = "failed"
                self._handle = None
            if self.run_dir:
                try:
                    self._write_json("metrics.json", self.snapshot())
                    from etl_parser import __version__

                    self._write_json(
                        "manifest.json",
                        {
                            "version": "1",
                            "run_id": self.run_id,
                            "status": self.status,
                            "framework_version": __version__,
                            "started_at": self.started_at,
                            "finished_at": datetime.now(UTC).isoformat(),
                            "event_files": self._opened_files,
                            "audit_complete": not self.sink_failed,
                            "payload_capture": False,
                            "configuration": self.configuration,
                        },
                    )
                except OSError:
                    self.status = "failed"
                    self.sink_failed = True
                    self.logger.error(
                        json.dumps({"event": "logging.summary_failed", "run_id": self.run_id})
                    )
            self._finished = True
            for handler in self.logger.handlers:
                handler.close()
            self.logger.handlers.clear()


def current_observer():
    """Return the :class:`RunObserver` for the current async/thread context, if any.

    Returns:
        RunObserver | None: The active observer, or ``None`` outside a run started
        with :func:`observed`.
    """
    return _CURRENT.get()


def observed(stage):
    """Build a decorator that wraps a function in a timed, observed run stage.

    The decorated function joins an existing run (via an ``observer`` keyword
    argument or one already active in the current context) when present, or else
    creates and owns a new :class:`RunObserver` from ``log_dir``/``log_level``/
    ``log_max_bytes``/``log_max_files`` keyword arguments. Every call runs inside
    :meth:`RunObserver.span` for ``stage``; any exception, including cancellation,
    is recorded and, for an owned observer, finalizes the run via
    :meth:`RunObserver.finish` before propagating. Works for both sync and async
    functions.

    Args:
        stage: Name of the stage/run recorded for the wrapped function, e.g.
            ``"analysis.run"``.

    Returns:
        Callable: A decorator that wraps a sync or async function with observation.
    """

    def decorate(function):
        """Wrap ``function`` with the observed-run behavior for ``observed``.

        Args:
            function: The sync or async function to wrap.

        Returns:
            Callable: ``async_wrapper`` if ``function`` is a coroutine function,
            otherwise ``wrapper``.
        """

        @contextmanager
        def scope(kwargs):
            """Enter the observed run scope for one call, given its keyword arguments.

            Resolves or creates the active :class:`RunObserver`, makes it current for
            the duration of the call, runs it inside a :meth:`RunObserver.span` for
            ``stage``, and finalizes an owned observer on exit.

            Args:
                kwargs: The wrapped call's keyword arguments, inspected for
                    ``observer``, ``log_dir``, ``log_level``, ``log_max_bytes`` and
                    ``log_max_files``.

            Yields:
                None.

            Raises:
                BaseException: Whatever exception the wrapped call raises, after
                    recording it.
            """
            observer = kwargs.get("observer") or current_observer()
            owned = observer is None
            if owned:
                observer = RunObserver(
                    log_dir=kwargs.get("log_dir"),
                    log_level=kwargs.get("log_level", "INFO"),
                    max_log_bytes=kwargs.get("log_max_bytes", 10_000_000),
                    max_log_files=kwargs.get("log_max_files", 20),
                )
            token = _CURRENT.set(observer)
            error = None
            try:
                if owned:
                    observer.event("run.started", command=stage)
                with observer.span(stage):
                    yield
            except BaseException as exc:
                error = exc
                raise
            finally:
                try:
                    if owned:
                        observer.finish(error=error)
                finally:
                    _CURRENT.reset(token)

        @wraps(function)
        def wrapper(*args, **kwargs):
            """Call the wrapped sync function inside an observed run scope.

            Args:
                *args: Positional arguments forwarded to the wrapped function.
                **kwargs: Keyword arguments forwarded to the wrapped function, and
                    inspected by :func:`scope` for observer/logging options.

            Returns:
                Whatever the wrapped function returns.
            """
            with scope(kwargs):
                return function(*args, **kwargs)

        @wraps(function)
        async def async_wrapper(*args, **kwargs):
            """Call the wrapped async function inside an observed run scope.

            Args:
                *args: Positional arguments forwarded to the wrapped function.
                **kwargs: Keyword arguments forwarded to the wrapped function, and
                    inspected by :func:`scope` for observer/logging options.

            Returns:
                Whatever the wrapped coroutine returns.
            """
            with scope(kwargs):
                return await function(*args, **kwargs)

        if inspect.iscoroutinefunction(function):
            return async_wrapper
        return wrapper

    return decorate
