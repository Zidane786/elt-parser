"""Explicitly opt-in live Anthropic-gateway ETL check. May incur model charges.

Credentials come only from ANTHROPIC_API_KEY or a hidden terminal prompt, never a file.
Use synthetic input by default; --source explicitly sends that source's selected files.
"""

import argparse
import getpass
import json
import os
import tempfile
from pathlib import Path

from etl_parser import AnalysisConfig, ParserClient
from etl_parser.workers.sql import DictSchemaProvider


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--execute",
        action="store_true",
        required=True,
        help="Explicitly authorize the bounded live model calls",
    )
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--source", type=Path)
    parser.add_argument("--schema", type=Path)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--ai-lineage", choices=["off", "fallback", "improve"], default="improve")
    parser.add_argument("--descriptions", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--background-comparison", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--max-calls", type=int, default=1)
    parser.add_argument("--max-output-tokens", type=int, default=16000)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    parser.add_argument("--deadline-seconds", type=float, default=3600)
    parser.add_argument("--include", action="append", help="AI file glob; repeat to select files")
    args = parser.parse_args()
    key = os.getenv("ANTHROPIC_API_KEY") or getpass.getpass("Gateway API key (hidden): ")
    root = args.out_dir or Path(tempfile.mkdtemp(prefix="etl-parser-gateway-"))
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="etl-parser-synthetic-") as temporary:
        source = args.source
        if source is None:
            source = Path(temporary) / "example.sql"
            source.write_text(
                "CREATE TABLE demo.output AS SELECT id, amount * 2 AS doubled FROM demo.input"
            )
        client = ParserClient(log_dir=root / "logs", log_level="ERROR")
        result = client.run(
            source,
            out_dir=root / "artifacts",
            schema=DictSchemaProvider(args.schema) if args.schema else None,
            config=AnalysisConfig(
                runner="anthropic",
                base_url=args.base_url,
                api_key=key,
                model=args.model,
                ai_lineage=args.ai_lineage,
                descriptions=args.descriptions,
                background_comparison=args.background_comparison,
                max_calls=args.max_calls,
                max_output_tokens=args.max_output_tokens,
                timeout_seconds=args.timeout_seconds,
                deadline_seconds=args.deadline_seconds,
                include=args.include or ["*"],
            ),
        )
        output = result.artifact_path
        summary = {
            "model": args.model,
            "runner": "anthropic",
            "extra_headers": False,
            "source": str(source) if args.source else "synthetic",
            "output": str(output),
            "log_run": str(result.log_path),
            "run_id": result.run_id,
            "ai_lineage": args.ai_lineage,
            "descriptions": args.descriptions,
            "baseline_jobs": len(result.baseline.jobs),
            "baseline_columns": len(result.baseline.column_edges),
            "effective_columns": len(result.document.column_edges),
            "unchanged_main": result.document == result.baseline,
            "reviewed_files": len(result.comparison["files"]),
            "warnings": result.warnings,
            "decisions": [
                {
                    "source_file": d["source_file"],
                    "status": d["status"],
                    "reason": d.get("reason"),
                    "validation_errors": d.get("validation_errors", []),
                    "http_status": d.get("http_status"),
                    "response_complete": d.get("response_complete"),
                    "unfilled_descriptions": len(d.get("unfilled_descriptions", [])),
                }
                for d in result.decisions
            ],
            "counters": result.metrics["counters"],
            "status": result.status,
        }
        (output / "live-check.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2), flush=True)
        if args.ai_lineage == "off" and result.document != result.baseline:
            raise RuntimeError("Invariant failed: AI-off changed main lineage")
        if result.warnings:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
