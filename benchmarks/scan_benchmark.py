"""Repeatable local performance probe, not an accuracy or production-capacity claim.

Run from the repository: python -m benchmarks.scan_benchmark --synthetic-files 500
or --source /path/to/etl --schema /path/to/catalog.json.
"""

import argparse
import json
import platform
import statistics
import tempfile
import time
import tracemalloc
from pathlib import Path

from etl_parser.pipeline import scan
from etl_parser.workers.sql import DictSchemaProvider


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--schema", type=Path)
    parser.add_argument("--synthetic-files", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repeats < 1 or args.synthetic_files < 1:
        parser.error("repeats and synthetic-files must be positive")
    schema = DictSchemaProvider(args.schema) if args.schema else None
    with tempfile.TemporaryDirectory(prefix="etl-parser-benchmark-") as temporary:
        root = Path(temporary)
        source = args.source
        if source is None:
            source = root / "sources"
            source.mkdir()
            for i in range(args.synthetic_files):
                (source / f"job_{i:05}.sql").write_text(
                    f"CREATE TABLE analytics.target_{i} AS SELECT id, "
                    f"amount * 2 AS doubled FROM raw.source_{i}",
                    encoding="utf-8",
                )
        results = {}
        baseline = None
        for mode in ("console_error", "persistent_debug"):
            samples, peaks = [], []
            for _ in range(args.repeats):
                tracemalloc.start()
                started = time.perf_counter()
                doc = scan(
                    source,
                    schema=schema,
                    log_level="ERROR",
                    log_dir=root / "logs" if mode == "persistent_debug" else None,
                ).document
                samples.append(time.perf_counter() - started)
                peaks.append(tracemalloc.get_traced_memory()[1])
                tracemalloc.stop()
                if baseline is None:
                    baseline = doc
                if doc != baseline:
                    raise RuntimeError("Benchmark runs produced different lineage")
            results[mode] = {
                "elapsed_seconds": samples,
                "median_seconds": statistics.median(samples),
                "peak_traced_python_bytes": max(peaks),
                "jobs_per_second": len(doc.jobs) / statistics.median(samples),
            }
        report = {
            "version": "1",
            "python": platform.python_version(),
            "platform": platform.platform(),
            "repeats": args.repeats,
            "source": str(args.source) if args.source else f"synthetic:{args.synthetic_files}",
            "jobs": len(baseline.jobs),
            "datasets": len(baseline.datasets),
            "table_edges": len(baseline.table_edges),
            "column_edges": len(baseline.column_edges),
            "unresolved": len(baseline.unresolved),
            "results": results,
            "persistent_log_time_ratio": results["persistent_debug"]["median_seconds"]
            / results["console_error"]["median_seconds"],
            "note": "Sequential warm-process local probe with tracemalloc overhead; "
            "memory is traced Python allocations, not RSS. Both modes instrument events. "
            "No AI/network calls; not a production capacity or independent accuracy score.",
        }
        text = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.write_text(text, encoding="utf-8")
        print(text, end="")


if __name__ == "__main__":
    main()
