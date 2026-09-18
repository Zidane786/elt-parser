import builtins
import json

from typer.testing import CliRunner

from etl_parser.ai_analysis import AnalysisConfig, analyze
from etl_parser.cli import app


def test_default_run_makes_no_sdk_calls_or_imports(tmp_path, monkeypatch):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.startswith(("agent_sdk", "boto3")):
            raise AssertionError("Default analysis must not load providers")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    result = analyze(source)
    assert result.document == result.baseline
    assert not result.work
    assert not result.changes


def test_unified_cli_defaults_and_artifacts(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    result = CliRunner().invoke(
        app,
        [
            "run",
            str(source),
            "--out-dir",
            str(tmp_path / "output"),
            "--log-dir",
            str(tmp_path / "logs"),
        ],
    )
    assert result.exit_code == 0, result.output
    output = json.loads(result.stdout)
    from pathlib import Path

    folder = Path(output["output"])
    assert (folder / "lineage.json").read_bytes() == (
        folder / "lineage.deterministic.json"
    ).read_bytes()
    assert not (folder / "lineage.ai.json").exists()
    assert output["ai_lineage"] == "off" and output["descriptions"] is False


def test_fallback_dry_run_needs_no_provider(tmp_path):
    source = tmp_path / "job.py"
    source.write_text('spark.table("db.s").select("x").custom().write.saveAsTable("db.t")')
    result = analyze(source, config=AnalysisConfig(ai_lineage="fallback", dry_run=True))
    assert result.work[0]["lineage_requested"]
    assert "partial_lineage" in result.work[0]["reasons"]
    assert result.document == result.baseline


def test_config_cli_override_and_invalid_options(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"ai_lineage": "improve", "descriptions": True, "dry_run": True}))
    result = CliRunner().invoke(
        app,
        [
            "run",
            str(source),
            "--config",
            str(config),
            "--ai-lineage",
            "off",
            "--no-descriptions",
            "--out-dir",
            str(tmp_path / "output"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert not json.loads(result.stdout)["descriptions"]
    result = CliRunner().invoke(app, ["run", str(source), "--ai-lineage", "invalid"])
    assert result.exit_code != 0


def test_multiple_proposals_keep_dataset_scope_when_one_schema_is_unknown():
    from etl_parser.ai_analysis import AnalysisResponse, _proposal_edges
    from etl_parser.models import Job, LineageDocument
    from etl_parser.observability import digest
    from etl_parser.scanner.repo import SourceFile
    from etl_parser.workers.sql import DictSchemaProvider

    source = SourceFile("job.py", 'df.write.saveAsTable("db.t")', ".py")
    job = Job(
        id="job",
        name="job",
        source_file=source.path,
        inputs=["s3://bucket/orders"],
        outputs=["glue://db/t"],
    )
    evidence = {
        "source_file": source.path,
        "source_digest": digest(source.text),
        "line_start": 1,
        "line_end": 1,
        "quote": source.text,
    }
    response = AnalysisResponse.model_validate(
        {
            "columns": [
                {
                    "job_id": "job",
                    "target": {"dataset_id": "glue://db/t", "name": name},
                    "sources": [{"dataset_id": "s3://bucket/orders", "name": name}],
                    "expression": name,
                    "kind": "identity",
                    "evidence": evidence,
                }
                for name in ("x", "y")
            ],
            "tables": [
                {
                    "job_id": "job",
                    "source": "s3://bucket/orders",
                    "target": "glue://db/t",
                    "evidence": evidence,
                }
            ],
        }
    )
    columns, tables, rejected = _proposal_edges(
        response,
        source,
        [job],
        LineageDocument(jobs=[job]),
        "request",
        "test",
        DictSchemaProvider({"db": {"t": ["x", "y"]}}),
    )
    assert len(columns) == 2 and len(tables) == 1 and not rejected
