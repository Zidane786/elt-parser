"""Shared helpers used by every worker: header parsing, cron normalisation, id plumbing.

Nothing here is specific to a language or dialect. ``SqlWorker`` (see design section 8.1)
and ``AirflowWorker`` (section 8.4) both use this module to read ``Key: value`` headers
from docstrings and SQL comments, to fold Airflow schedule strings and cron comments to a
canonical five-field cron, and to compute the repo-relative ``Job.id`` every worker uses as
the join key between jobs, schedules, and tasks.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from etl_parser.models import Schedule

HEADER_KEYS = {"source", "target", "owner", "grain", "schedule", "dependencies", "description"}
_HEADER_LINE = re.compile(r"^\s*(?:--|#)?\s*(?P<key>[A-Za-z][A-Za-z _]*):\s*(?P<value>.+?)\s*$")

CRON_PRESETS = {
    "@once": None,
    "@hourly": "0 * * * *",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@weekly": "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@quarterly": "0 0 1 */3 *",
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@continuous": None,
    "hourly": "0 * * * *",
    "daily": "0 0 * * *",
    "weekly": "0 0 * * 0",
    "monthly": "0 0 1 * *",
}
_CRON_5 = re.compile(r"^(\S+\s+){4}\S+$")
_DAILY_AT = re.compile(r"^daily\s+(\d{1,2}):(\d{2})(?:\s+UTC)?$", re.I)
_EVERY_N = re.compile(r"^every\s+(\d+)\s*(minute|min|hour|day)s?", re.I)


def parse_header(text: str) -> dict[str, str]:
    """Extract ``Key: value`` lines from a module docstring or SQL comment header.

    A job's ``.py`` or ``.sql`` source often carries a metadata header (owner, schedule,
    description, ...) as the module docstring or a leading block of ``--``/``#`` comments.
    Workers call this to recover that metadata without executing the file.

    Args:
        text: Full source text of the ``.py`` or ``.sql`` file.

    Returns:
        Mapping of lower-cased header key (restricted to ``HEADER_KEYS``) to its value.
        Only the leading block of the text is inspected, so a ``Source:`` line appearing
        later in prose does not count. The first occurrence of a key wins.
    """
    out: dict[str, str] = {}
    lines: list[str] = []
    try:
        doc = ast.get_docstring(ast.parse(text))
    except SyntaxError:
        doc = None
    if doc:
        lines = doc.splitlines()
    else:
        for raw in text.splitlines():
            stripped = raw.strip()
            if not stripped:
                continue
            if stripped.startswith(("--", "#")):
                lines.append(stripped.lstrip("-#").strip())
            else:
                break
    for line in lines:
        m = _HEADER_LINE.match(line)
        if not m:
            continue
        key = m.group("key").strip().lower()
        if key in HEADER_KEYS and key not in out:
            out[key] = m.group("value").strip()
    return out


def first_docstring_line(text: str) -> str | None:
    """Return the first non-empty line of a module docstring, used as a job description.

    Args:
        text: Python source text to parse for a module docstring.

    Returns:
        The stripped first non-empty line of the docstring, or ``None`` when the text has
        no docstring, is not parseable Python, or the docstring is empty.
    """
    try:
        doc = ast.get_docstring(ast.parse(text))
    except SyntaxError:
        return None
    if doc:
        return next((line.strip() for line in doc.splitlines() if line.strip()), None)
    return None


def normalize_cron(text: str | None) -> str | None:
    """Normalise a human or Airflow schedule string to five-field cron, best-effort.

    Used by ``AirflowWorker`` to turn ``schedule``/``schedule_interval`` text and by
    ``comment_schedule`` to turn a ``Schedule:`` header value into the canonical cron
    stored on a ``Schedule``. Recognises Airflow presets (``@daily``, ``@hourly``, ...),
    literal five-field cron, ``"daily HH:MM"``, and ``"every N minute/hour/day"`` phrasing.

    Args:
        text: Raw schedule text, or ``None``.

    Returns:
        A normalised five-field cron string, or ``None`` when ``text`` is ``None``, empty,
        a preset with no fixed cron equivalent (e.g. ``@once``), a ``timedelta``-style
        interval (anchored, not a wall-clock schedule), or otherwise cannot be resolved.

    Example:
        >>> normalize_cron("@daily")
        '0 0 * * *'
        >>> normalize_cron("every 15 minutes")
        '*/15 * * * *'
    """
    if text is None:
        return None
    t = text.strip().strip('"').strip("'")
    if not t:
        return None
    low = t.lower()
    if low in CRON_PRESETS:
        return CRON_PRESETS[low]
    if _CRON_5.fullmatch(t) and _valid_cron(t):
        return " ".join(t.split())
    m = _DAILY_AT.match(low)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
        return f"{minute} {hour} * * *" if hour < 24 and minute < 60 else None
    m = _EVERY_N.fullmatch(low)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit.startswith("min"):
            return f"*/{n} * * * *" if 0 < n < 60 and 60 % n == 0 else None
        if unit == "hour":
            return f"0 */{n} * * *" if 0 < n < 24 and 24 % n == 0 else None
        if unit == "day":
            return "0 0 * * *" if n == 1 else None
    # A timedelta is anchored to its start time; cron is a wall-clock schedule.
    # Preserve the original interval_text without claiming they are equivalent.
    return None


def _valid_cron(text: str) -> bool:
    """Check that a five-field cron string has fields within their valid ranges.

    Args:
        text: A whitespace-separated five-field cron string (minute hour day month weekday).
            Month and weekday names (``JAN``, ``MON``, ...) are accepted.

    Returns:
        ``True`` when every field is ``*``, a bounded value, a bounded range, or a bounded
        step, all within the field's valid range; ``False`` otherwise.
    """
    bounds = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]
    names = {
        name: str(i)
        for i, name in enumerate(
            ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], 1
        )
    }
    days = {
        name: str(i) for i, name in enumerate(["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"])
    }
    for index, (field, (low, high)) in enumerate(zip(text.upper().split(), bounds, strict=True)):
        if index in (3, 4):
            for name, value in (names if index == 3 else days).items():
                field = field.replace(name, value)
        for item in field.split(","):
            base, slash, step = item.partition("/")
            if slash and (not step.isdigit() or not 0 < int(step) <= high - low + 1):
                return False
            if base == "*":
                continue
            ends = base.split("-")
            if len(ends) > 2 or any(not x.isdigit() or not low <= int(x) <= high for x in ends):
                return False
            if len(ends) == 2 and int(ends[0]) > int(ends[1]):
                return False
    return True


def comment_schedule(job_id: str, text: str, source_file: str | None) -> Schedule:
    """Build a ``Schedule`` from a job header's ``Schedule:`` value.

    Used by ``SqlWorker.analyze_file`` when ``parse_header`` finds a ``schedule`` key, so a
    job with no Airflow DAG or ``product.yaml`` entry still gets a recorded cadence.

    Args:
        job_id: Id of the job the schedule belongs to.
        text: Raw ``Schedule:`` header value, passed through ``normalize_cron``.
        source_file: Repo-relative path of the file the header was read from.

    Returns:
        A ``Schedule`` with orchestrator ``"cron_comment"``, id ``f"comment.{job_id}"``.
    """
    return Schedule(
        id=f"comment.{job_id}",
        orchestrator="cron_comment",
        cron=normalize_cron(text),
        interval_text=text,
        source_file=source_file,
    )


def repo_relative(path: Path, root: Path | None) -> str:
    """Return ``path`` relative to ``root``, or the raw path when that is not possible.

    Args:
        path: Absolute or relative path to a source file.
        root: Repo root to make ``path`` relative to, or ``None``.

    Returns:
        The path as a string relative to ``root``, or ``str(path)`` unchanged when ``root``
        is ``None`` or ``path`` does not lie under it.
    """
    try:
        return str(path.resolve().relative_to(root.resolve())) if root else str(path)
    except ValueError:
        return str(path)


def job_id_for(path: Path, root: Path | None) -> str:
    """Derive a job id from a source file path: its repo-relative path minus the extension.

    Args:
        path: Path to a ``.py`` or ``.sql`` source file.
        root: Repo root used to make the path relative; see ``repo_relative``.

    Returns:
        Forward-slash-separated repo-relative path with a trailing ``.py`` or ``.sql``
        stripped, used as ``Job.id`` throughout the pipeline.
    """
    rel = repo_relative(path, root)
    return re.sub(r"\.(py|sql)$", "", rel).replace("\\", "/")
