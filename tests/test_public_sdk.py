import asyncio
import json
import threading

import pytest
from typer.testing import CliRunner

from etl_parser import AnalysisConfig, ParserClient, analyze, analyze_async
from etl_parser.cli import app
from etl_parser.observability import current_observer


def source_file(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    return source


def test_public_sync_result_and_matching_artifact_log_identity(tmp_path):
    client = ParserClient(log_dir=tmp_path / "logs", log_level="ERROR")
    result = client.run(source_file(tmp_path), out_dir=tmp_path / "artifacts")
    assert len(result.document.column_edges) == 1
    assert result.run_id == result.metrics["run_id"]
    assert result.status == "success"
    assert result.artifact_path.name == f"run_{result.run_id}"
    assert json.loads((result.log_path / "metrics.json").read_text())["run_id"] == result.run_id
    manifest = json.loads((result.artifact_path / "manifest.json").read_text())
    assert manifest["run_id"] == result.run_id
    assert result.metrics["counters"]["artifacts.written"] >= 6
    assert "index" not in result.to_dict()
    json.dumps(result.to_dict())
    assert current_observer() is None


def test_public_async_concurrent_runs_are_isolated(tmp_path):
    source = source_file(tmp_path)
    client = ParserClient(log_dir=tmp_path / "logs", log_level="ERROR")

    async def run():
        with pytest.raises(RuntimeError, match="arun"):
            client.run(source)
        first, second = await asyncio.gather(client.arun(source), client.arun(source))
        assert first.run_id != second.run_id
        assert first.log_path != second.log_path
        assert first.metrics["counters"]["files.parsed"] == 1
        assert second.metrics["counters"]["files.parsed"] == 1
        first.warnings.append("local")
        assert not second.warnings
        assert current_observer() is None

    asyncio.run(run())


def test_async_scan_does_not_block_event_loop(monkeypatch, tmp_path):
    import etl_parser.ai_analysis as module

    started, release = threading.Event(), threading.Event()
    original = module.scan

    def blocking_scan(*args, **kwargs):
        started.set()
        assert release.wait(timeout=2), "event loop could not release scanner"
        return original(*args, **kwargs)

    monkeypatch.setattr(module, "scan", blocking_scan)
    source = source_file(tmp_path)

    async def run():
        task = asyncio.create_task(analyze_async(source, log_level="ERROR"))
        try:
            assert await asyncio.to_thread(started.wait, 1)
        finally:
            release.set()
        return await task

    assert asyncio.run(run()).document.column_edges


def test_defaults_overrides_and_no_files_by_default(tmp_path):
    config = AnalysisConfig(api_key="PRIVATE_TEST_KEY", extra_headers={"x-test": "PRIVATE_HEADER"})
    assert config.max_output_tokens == 16000
    assert config.timeout_seconds == 300 and config.deadline_seconds == 3600
    client = ParserClient(config=config, log_level="ERROR")
    config.max_output_tokens = 7  # Client snapshots caller configuration.
    source = source_file(tmp_path)
    result = client.run(source)
    assert result.configuration["max_output_tokens"] == 16000
    assert not result.artifact_path and not result.log_path
    assert "PRIVATE_TEST_KEY" not in json.dumps(result.to_dict())
    assert "PRIVATE_HEADER" not in json.dumps(result.to_dict())
    override = client.run(source, config={"max_output_tokens": 2222})
    assert override.configuration["max_output_tokens"] == 2222
    assert client.run(source).configuration["max_output_tokens"] == 16000
    assert list(tmp_path.iterdir()) == [source]
    assert analyze(source, log_level="ERROR").document.column_edges


def test_cli_token_override_and_help(tmp_path):
    result = CliRunner().invoke(
        app,
        [
            "run",
            str(source_file(tmp_path)),
            "--max-output-tokens",
            "2222",
            "--out-dir",
            str(tmp_path / "artifacts"),
            "--log-level",
            "ERROR",
        ],
    )
    assert result.exit_code == 0, result.output
    manifest = next((tmp_path / "artifacts").glob("*/manifest.json"))
    assert json.loads(manifest.read_text())["configuration"]["max_output_tokens"] == 2222
    assert "16000" in CliRunner().invoke(app, ["run", "--help"]).output


def test_sdk_failure_finalizes_logs(tmp_path):
    client = ParserClient(log_dir=tmp_path / "logs", log_level="ERROR")
    with pytest.raises((FileNotFoundError, ValueError)):
        client.run(tmp_path / "absent")
    manifest = next((tmp_path / "logs").glob("*/manifest.json"))
    assert json.loads(manifest.read_text())["status"] == "failed"
    assert current_observer() is None


def test_sdk_cancellation_finalizes_logs_and_keeps_injected_runner_open(tmp_path):
    pytest.importorskip("agent_sdk")

    class WaitingRunner:
        closed = False

        async def complete(self, **kwargs):
            started.set()
            await asyncio.Event().wait()

        async def aclose(self):
            self.closed = True

    source = source_file(tmp_path)
    runner = WaitingRunner()
    client = ParserClient(
        config=AnalysisConfig(ai_lineage="improve", model="test"),
        log_dir=tmp_path / "logs",
        log_level="ERROR",
    )

    async def run():
        nonlocal started
        started = asyncio.Event()
        task = asyncio.create_task(client.arun(source, runner=runner))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert current_observer() is None

    started = None
    asyncio.run(run())
    assert not runner.closed
    manifest = next((tmp_path / "logs").glob("*/manifest.json"))
    assert json.loads(manifest.read_text())["status"] == "cancelled"


def test_deadline_skip_is_distinct_from_call_limit(tmp_path):
    class NeverCalled:
        """Stands in for a configured provider the budget policy must never reach."""

        async def complete(self, **kwargs):
            raise AssertionError("Budget policy must not reach the provider")

    result = ParserClient(
        config=AnalysisConfig(
            ai_lineage="improve",
            model="test",
            deadline_seconds=1e-12,
        ),
        log_level="ERROR",
    ).run(source_file(tmp_path), runner=NeverCalled())
    assert result.decisions[0]["reason"] == "deadline_exceeded"
    assert result.metrics["gauges"]["ai.calls.total"] == 0
    assert result.status == "partial"
