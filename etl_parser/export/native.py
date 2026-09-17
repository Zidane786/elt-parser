"""Deterministic native JSON serialization."""

import json
from pathlib import Path

from etl_parser.models import LineageDocument
from etl_parser.observability import current_observer, digest


def write_native(document: LineageDocument, path: Path | str):
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
    return LineageDocument.model_validate_json(Path(path).read_text(encoding="utf-8"))
