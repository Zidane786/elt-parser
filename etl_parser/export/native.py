"""Deterministic native JSON serialization."""

import json
from pathlib import Path

from etl_parser.models import LineageDocument


def write_native(document: LineageDocument, path: Path | str):
    Path(path).write_text(
        json.dumps(document.sorted().model_dump(mode="json"), sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def read_native(path: Path | str) -> LineageDocument:
    return LineageDocument.model_validate_json(Path(path).read_text(encoding="utf-8"))
