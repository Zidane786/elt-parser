import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor

import pytest
from typer.testing import CliRunner

from etl_parser.cli import app
from etl_parser.observability import RunObserver, current_observer, observed, sanitize
from etl_parser.pipeline import scan


def artifacts(log_dir):
    directories = list(log_dir.iterdir())
    assert len(directories) == 1
    folder = directories[0]
    events = [
        json.loads(line)
        for file in sorted(folder.glob("events*.jsonl"))
        for line in file.read_text().splitlines()
    ]
    return events, json.loads((folder / "metrics.json").read_text()), folder


def test_scan_logs_findings_without_changing_graph(tmp_path, capsys):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x + 1 AS y FROM db.s")
    expected = scan(source).document.model_dump()
    capsys.readouterr()
    doc = scan(source, log_dir=tmp_path / "logs").document
    output = capsys.readouterr()
    assert not output.out
    assert "scan.completed" in output.err
    events, metrics, folder = artifacts(tmp_path / "logs")
    assert expected == doc.model_dump()
    assert {"job.found", "lineage.table_found", "lineage.column_found"} <= {
        e["event"] for e in events
    }
    assert len({e["run_id"] for e in events}) == 1
    assert len({e["event_id"] for e in events}) == len(events)
    assert metrics["gauges"]["lineage.column_edges"] == 1
    assert metrics["counters"]["files.parsed"] == 1
    assert metrics["durations"]["parser.sql"]["count"] == 1
    assert metrics["status"] == "success"
    assert metrics["accuracy"] is None
    assert metrics["cost_estimate"] is None
    assert json.loads((folder / "manifest.json").read_text())["audit_complete"]
    assert current_observer() is None


def test_cli_stdout_remains_json_and_export_joins_run(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    result = CliRunner().invoke(
        app,
        [
            "scan",
            str(source),
            "--out",
            str(tmp_path / "out.json"),
            "--log-dir",
            str(tmp_path / "logs"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["jobs"] == 1
    assert "run.started" in result.stderr
    events, metrics, _ = artifacts(tmp_path / "logs")
    assert sum(e["event"] == "run.started" for e in events) == 1
    assert sum(e["event"] == "run.finished" for e in events) == 1
    assert metrics["counters"]["artifacts.written"] == 1


def test_uncertain_scan_is_partial_and_strict_cli_failure_is_recorded(tmp_path):
    source = tmp_path / "job.sql"
    source.write_text("SELECT FROM")
    result = CliRunner().invoke(
        app,
        [
            "scan",
            str(source),
            "--out",
            str(tmp_path / "out.json"),
            "--log-dir",
            str(tmp_path / "logs"),
        ],
    )
    assert result.exit_code == 1
    events, metrics, _ = artifacts(tmp_path / "logs")
    assert metrics["status"] == "failed"
    assert metrics["counters"]["diagnostics.unsupported_syntax"] >= 1
    assert any(e["event"] == "diagnostic.found" for e in events)


def test_metadata_redaction_and_no_source_payloads(tmp_path, monkeypatch, capsys):
    secret = "DO_NOT_LOG_TEST_SECRET"
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)
    observer = RunObserver(log_dir=tmp_path / "logs", log_level="DEBUG")
    observer.event(
        "test",
        password=secret,
        source_text="PRIVATE SQL HERE",
        prompt="PRIVATE PROMPT",
        response="PRIVATE RESPONSE",
        headers={"x": secret},
        metadata={"link": "postgres://user:password@db/t", "note": secret},
        message="Bearer token-value\nInjected event",
    )
    observer.finish()
    output = capsys.readouterr().err
    events, _, folder = artifacts(tmp_path / "logs")
    combined = output + "".join(f.read_text() for f in folder.iterdir())
    for value in (
        secret,
        "PRIVATE SQL HERE",
        "PRIVATE PROMPT",
        "PRIVATE RESPONSE",
        "user:password",
        "token-value",
    ):
        assert value not in combined
    assert events[0]["password"] == "[redacted]"
    assert len(output.splitlines()) == 2  # Embedded newline cannot inject a log event.


def test_source_literals_and_exception_messages_never_enter_event_logs(tmp_path):
    source = tmp_path / "job.py"
    source.write_text('spark.sql("SELECT \'PRIVATE_LITERAL\' AS value").write.saveAsTable("db.t")')
    scan(source, log_dir=tmp_path / "logs")
    _, _, folder = artifacts(tmp_path / "logs")
    assert "PRIVATE_LITERAL" not in "".join(f.read_text() for f in folder.iterdir())


def test_rotation_is_bounded_and_reports_lost_persistence(tmp_path, capsys):
    observer = RunObserver(log_dir=tmp_path / "logs", max_log_bytes=1024, max_log_files=2)
    for i in range(30):
        observer.event("detail", level="DEBUG", sequence=i, note="x" * 500)
    observer.finish()
    events, metrics, folder = artifacts(tmp_path / "logs")
    assert len(list(folder.glob("events*.jsonl"))) == 2
    assert events
    assert metrics["status"] == "partial"
    assert metrics["counters"]["events.not_persisted"] > 0
    assert not json.loads((folder / "manifest.json").read_text())["audit_complete"]
    assert "logging.persistence_failed" in capsys.readouterr().err


def test_thread_safe_events_and_bounded_duration_samples(tmp_path):
    observer = RunObserver(log_dir=tmp_path / "logs", log_level="ERROR")

    def emit(i):
        observer.count("work")
        observer.duration("work", i + 1)
        observer.event("work", level="DEBUG", item=i)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(emit, range(500)))
    observer.finish()
    events, metrics, _ = artifacts(tmp_path / "logs")
    assert metrics["counters"]["work"] == 500
    assert metrics["durations"]["work"]["count"] == 500
    assert metrics["durations"]["work"]["recent_sample_count"] == 256
    assert len({e["event_id"] for e in events}) == 501


def test_failure_finalizes_and_restores_context(tmp_path):
    @observed("failure")
    def fail(*, log_dir):
        raise ValueError("PRIVATE EXCEPTION CONTENT")

    with pytest.raises(ValueError):
        fail(log_dir=tmp_path / "logs")
    events, metrics, folder = artifacts(tmp_path / "logs")
    assert metrics["status"] == "failed"
    assert events[-1]["error_type"] == "ValueError"
    assert "PRIVATE EXCEPTION CONTENT" not in "".join(f.read_text() for f in folder.iterdir())
    assert current_observer() is None


def test_async_cancellation_finalizes_and_restores_context(tmp_path):
    @observed("cancel")
    async def cancel(*, log_dir):
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cancel(log_dir=tmp_path / "logs"))
    _, metrics, _ = artifacts(tmp_path / "logs")
    assert metrics["status"] == "cancelled"
    assert current_observer() is None


def test_does_not_change_application_logging_or_reuse_run_files(tmp_path):
    root = logging.getLogger()
    handlers = list(root.handlers)
    level = root.level
    for _ in range(2):
        observer = RunObserver(log_dir=tmp_path / "logs")
        observer.event("hello")
        observer.finish()
        observer.finish()
    assert len(list((tmp_path / "logs").iterdir())) == 2
    assert root.handlers == handlers
    assert root.level == level


def test_validation_and_explicit_metadata_truncation():
    with pytest.raises(ValueError, match="log_level"):
        RunObserver(log_level="INVALID")
    with pytest.raises(ValueError, match="limits"):
        RunObserver(max_log_files=0)
    assert sanitize(list(range(105)))[-1] == {"omitted_items": 5}
    assert sanitize(float("nan")) is None


def test_event_sink_disk_error_is_visible_and_does_not_hide_parser_results(tmp_path, capsys):
    observer = RunObserver(log_dir=tmp_path / "logs")
    observer._handle.close()

    class FullDisk:
        def write(self, value):
            raise OSError("SENSITIVE OS MESSAGE")

        def close(self):
            raise OSError("SENSITIVE CLOSE MESSAGE")

    observer._handle = FullDisk()
    observer.event("test")
    assert observer.sink_failed
    observer.finish()
    _, metrics, folder = artifacts(tmp_path / "logs")
    assert metrics["status"] == "partial"
    assert metrics["counters"]["logging.failures"] == 1
    output = capsys.readouterr().err
    assert "logging.persistence_failed" in output
    assert "SENSITIVE" not in output
    assert not json.loads((folder / "manifest.json").read_text())["audit_complete"]


def test_summary_write_failure_is_reported_without_masking_exception(tmp_path, monkeypatch, capsys):
    observer = RunObserver(log_dir=tmp_path / "logs")

    def fail_summary(*args):
        raise OSError("SENSITIVE SUMMARY ERROR")

    monkeypatch.setattr(observer, "_write_json", fail_summary)
    observer.finish()
    assert observer.status == "failed"
    assert observer.sink_failed
    assert "logging.summary_failed" in capsys.readouterr().err


def test_close_failure_cannot_publish_complete_audit_manifest(tmp_path):
    observer = RunObserver(log_dir=tmp_path / "logs")
    handle = observer._handle

    class DelayedFailure:
        write = handle.write
        flush = handle.flush

        def close(self):
            handle.close()
            raise OSError("delayed close failure")

    observer._handle = DelayedFailure()
    observer.finish()
    _, metrics, folder = artifacts(tmp_path / "logs")
    assert metrics["status"] == "failed"
    assert not json.loads((folder / "manifest.json").read_text())["audit_complete"]
