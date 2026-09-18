"""Deterministic native JSON serialization: ``lineage.json`` (spec sections 10, 14).

Implements the ``NativeExporter`` role: writes the full :class:`~etl_parser.models.
LineageDocument` sorted and with indentation so re-running a scan on the same commit
produces byte-identical output, and reads it back for downstream commands (impact,
products, export, describe).
"""

import json
from pathlib import Path

from etl_parser.models import LineageDocument
from etl_parser.observability import current_observer, digest


def write_native(document: LineageDocument, path: Path | str):
    """Serialize a lineage document to deterministic, sorted JSON and write it to disk.

    Args:
        document: The lineage document to write. It is sorted (see
            :meth:`~etl_parser.models.LineageDocument.sorted`) before serialization; the
            document passed in is not mutated.
        path: Destination file path.
    """
    text = json.dumps(document.sorted().model_dump(mode="json"), sort_keys=True, indent=2) + "\n"
    Path(path).write_text(text, encoding="utf-8")
    observer = current_observer()
    if observer:
        observer.count("artifacts.written")
        observer.count("artifacts.bytes", len(text.encode("utf-8")))
        observer.event(
            "artifact.written",
            path=str(path),
            kind="native_lineage",
            size_bytes=len(text.encode("utf-8")),
            content_digest=digest(text),
        )


def read_native(path: Path | str) -> LineageDocument:
    """Read and validate a native ``lineage.json`` file.

    Args:
        path: Path to a file previously written by :func:`write_native`.

    Returns:
        LineageDocument: The parsed and validated document.

    Raises:
        pydantic.ValidationError: If the file's JSON does not match the
            :class:`~etl_parser.models.LineageDocument` schema.
    """
    return LineageDocument.model_validate_json(Path(path).read_text(encoding="utf-8"))
