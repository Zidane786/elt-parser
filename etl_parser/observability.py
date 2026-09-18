"""Run-scoped events and bounded metrics. No provider SDK or global logger setup.

Console events go to stderr; optional JSONL files contain more detailed metadata.
Source code, prompts, responses, credentials and exception messages are not logged.
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
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sanitize(value, *, secrets=(), depth=0):
    """Best-effort metadata redaction, not permission to log arbitrary source text."""
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
    """Thread-safe streaming event sink with no unbounded event history in memory."""

    def __init__(
        self,
        *,
        log_dir=None,
        log_level="INFO",
        max_log_bytes=10_000_000,
        max_log_files=20,
        secrets=(),
    ):
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
        name = "events.jsonl" if self._file_number == 0 else f"events.{self._file_number:03}.jsonl"
        fd = os.open(self.run_dir / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        self._handle = os.fdopen(fd, "w", encoding="utf-8")
        self._opened_files += 1
        self._file_bytes = 0

    def count(self, name, amount=1):
        with self._lock:
            self.counters[name] += amount

    def gauge(self, name, value):
        with self._lock:
            self.gauges[name] = value

    def configure(self, **values):
        with self._lock:
            self.configuration.update(sanitize(values, secrets=self._secrets))

    def protect(self, *secrets):
        """Register explicitly supplied secrets before emitting provider metadata."""
        with self._lock:
            self._secrets += tuple(value for value in secrets if value)

    def partial(self):
        with self._lock:
            if self.status == "success":
                self.status = "partial"

    def duration(self, name, milliseconds):
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
    return _CURRENT.get()


def observed(stage):
    """Join an existing run or create one; all exceptions/cancellation finalize it."""

    def decorate(function):
        @contextmanager
        def scope(kwargs):
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
            with scope(kwargs):
                return function(*args, **kwargs)

        @wraps(function)
        async def async_wrapper(*args, **kwargs):
            with scope(kwargs):
                return await function(*args, **kwargs)

        if inspect.iscoroutinefunction(function):
            return async_wrapper
        return wrapper

    return decorate
