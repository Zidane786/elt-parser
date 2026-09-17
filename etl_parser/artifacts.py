"""Unique run output directories with atomic, owner-private artifact files."""

import json
import os
import tempfile
from pathlib import Path
from uuid import uuid4

from etl_parser.observability import current_observer, digest


def write_analysis(result, directory):
    observer = current_observer()
    run_id = observer.run_id if observer else uuid4().hex
    folder = Path(directory) / f"run_{run_id}"
    folder.mkdir(parents=True, exist_ok=False, mode=0o700)
    files = {
        "lineage.json": result.document.sorted().model_dump(mode="json"),
        "lineage.deterministic.json": result.baseline.sorted().model_dump(mode="json"),
        "catalog.json": result.catalog,
        "decisions.json": result.decisions,
        "changes.json": result.changes,
        "work-plan.json": result.work,
    }
    if (
        result.comparison["files"]
        or result.ai_document.column_edges
        or result.ai_document.table_edges
    ):
        files["lineage.ai.json"] = result.ai_document.sorted().model_dump(mode="json")
        files["lineage.comparison.json"] = result.comparison
    hashes = {}
    for name, value in files.items():
        text = json.dumps(value, sort_keys=True, indent=2) + "\n"
        fd, temporary = tempfile.mkstemp(prefix=".artifact-", dir=folder)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(text)
            os.replace(temporary, folder / name)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        hashes[name] = digest(text)
        if observer:
            observer.count("artifacts.written")
            observer.count("artifacts.bytes", len(text.encode("utf-8")))
            observer.event("artifact.written", path=str(folder / name), content_digest=hashes[name])
    manifest = {
        "version": "1",
        "run_id": run_id,
        "source": result.index.origin,
        "revision": result.index.revision,
        "source_digests": {source.path: digest(source.text) for source in result.index.files},
        "configuration": result.configuration,
        "files": hashes,
        "status": observer.status if observer else result.status,
        "warnings": result.warnings,
        "note": "No live correctness guarantee; review unresolved items and AI changes.",
    }
    # Completion marker is written last; a missing marker means interrupted export.
    fd, temporary = tempfile.mkstemp(prefix=".manifest-", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(manifest, stream, sort_keys=True, indent=2)
            stream.write("\n")
        os.replace(temporary, folder / "manifest.json")
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return folder
