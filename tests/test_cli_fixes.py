"""CLI, pipeline, packaging and documentation fixes from WP-F of the review plan.

Covers exit-code gating by unresolved kind, provider authentication/authorization exit
codes, missing-file parameter errors, export parent creation, the library logging
default, option help strings, dependency declarations and public-repo hygiene.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from etl_parser.cli import app
from etl_parser.models import Job, LineageDocument, Unresolved

ROOT = Path(__file__).resolve().parents[1]
NON_GATING = ("analysis_note", "skipped_entry", "missing_in_source")


def document(*kinds):
    """Build a lineage document carrying one unresolved item per requested kind.

    Args:
        *kinds: ``Unresolved.kind`` values to include.

    Returns:
        LineageDocument: A document with one job and the requested diagnostics.
    """
    return LineageDocument(
        jobs=[Job(id="job:1", name="job", source_file="job.py", language="python")],
        unresolved=[
            Unresolved(kind=kind, source_file="job.py", reason="example") for kind in kinds
        ],
    )


@pytest.fixture
def scanned(monkeypatch):
    """Patch the CLI's scan entry point with a constructed document.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        Callable[[LineageDocument], None]: Installs the document the next scan returns.
    """

    def install(doc):
        """Make ``etl-parser scan`` return ``doc`` without touching the filesystem."""

        class Graph:
            """Minimal stand-in for :class:`~etl_parser.graph.builder.LineageGraph`."""

            def to_document(self):
                """Return the constructed document."""
                return doc

        monkeypatch.setattr("etl_parser.cli.scan_repository", lambda *a, **k: Graph())

    return install


@pytest.fixture
def analyzed(monkeypatch):
    """Patch the CLI's analysis entry point with a constructed run result.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        Callable[..., None]: Installs the document, decisions and warnings ``run`` sees.
    """

    def install(doc, *, decisions=(), warnings=()):
        """Make ``etl-parser run`` return a result built from the given parts."""

        class Result:
            """Minimal stand-in for :class:`~etl_parser.ai_analysis.AnalysisRun`."""

            def __init__(self):
                self.document = doc
                self.decisions = list(decisions)
                self.warnings = list(warnings)

        monkeypatch.setattr("etl_parser.ai_analysis.analyze", lambda *a, **k: Result())
        monkeypatch.setattr("etl_parser.artifacts.write_analysis", lambda result, out: Path(out))

    return install


@pytest.mark.parametrize("kind", NON_GATING)
def test_scan_does_not_gate_on_non_blocking_unresolved_kinds(kind, scanned, tmp_path):
    scanned(document(kind))
    result = CliRunner().invoke(app, ["scan", str(tmp_path), "--out", str(tmp_path / "out.json")])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout)["unresolved"] == 1


def test_scan_gates_on_unsupported_syntax_only(scanned, tmp_path):
    scanned(document("unsupported_syntax", *NON_GATING))
    result = CliRunner().invoke(app, ["scan", str(tmp_path), "--out", str(tmp_path / "out.json")])
    assert result.exit_code == 1, result.output


@pytest.mark.parametrize("kind", NON_GATING)
def test_run_does_not_gate_on_non_blocking_unresolved_kinds(kind, analyzed, tmp_path):
    analyzed(document(kind))
    result = CliRunner().invoke(app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out")])
    assert result.exit_code == 0, result.output


def test_run_gates_on_unsupported_syntax(analyzed, tmp_path):
    analyzed(document("unsupported_syntax"))
    result = CliRunner().invoke(app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out")])
    assert result.exit_code == 1, result.output


@pytest.mark.parametrize(
    "reason", ["provider_authentication_failed", "provider_authorization_failed"]
)
def test_run_exits_two_on_provider_authentication_failure_without_strict(
    reason, analyzed, tmp_path
):
    analyzed(
        document(),
        decisions=[{"source": "job.py", "status": "failed", "reason": reason, "http_status": 401}],
        warnings=[f"job.py: {reason} (ProviderError)"],
    )
    result = CliRunner().invoke(app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out")])
    assert result.exit_code == 2, result.output


def test_run_provider_authentication_failure_outranks_unsupported_syntax(analyzed, tmp_path):
    analyzed(
        document("unsupported_syntax"),
        decisions=[{"status": "failed", "reason": "provider_authentication_failed"}],
    )
    result = CliRunner().invoke(app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out")])
    assert result.exit_code == 2, result.output


def test_run_other_provider_failures_do_not_exit_two(analyzed, tmp_path):
    analyzed(
        document(),
        decisions=[{"status": "failed", "reason": "provider_rate_limited", "http_status": 429}],
        warnings=["job.py: provider_rate_limited (ProviderError)"],
    )
    result = CliRunner().invoke(app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out")])
    assert result.exit_code == 0, result.output


def test_run_strict_still_fails_for_warnings_and_non_gating_kinds(analyzed, tmp_path):
    analyzed(document("analysis_note"), warnings=["job.py: something partial"])
    arguments = ["run", str(tmp_path), "--out-dir", str(tmp_path / "out")]
    assert CliRunner().invoke(app, arguments).exit_code == 0
    assert CliRunner().invoke(app, [*arguments, "--strict"]).exit_code == 1


def test_run_strict_exit_two_takes_precedence_over_strict_exit_one(analyzed, tmp_path):
    analyzed(document(), decisions=[{"reason": "provider_authentication_failed"}])
    result = CliRunner().invoke(
        app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out"), "--strict"]
    )
    assert result.exit_code == 2, result.output


@pytest.fixture
def lineage_file(tmp_path):
    """Write a small valid native lineage file.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Path: The written ``lineage.json``.
    """
    from etl_parser.export.native import write_native

    path = tmp_path / "lineage.json"
    write_native(document(), path)
    return path


MISSING = "no/such/file.json"


@pytest.mark.parametrize(
    "arguments",
    [
        ["export", "catalog", MISSING, "--out", "out.json"],
        ["export", "openlineage", MISSING, "--out", "events"],
        ["impact", MISSING, "glue://a/b"],
        ["products", MISSING],
    ],
)
def test_missing_lineage_file_is_a_parameter_error(arguments, tmp_path):
    result = CliRunner().invoke(app, arguments)
    assert result.exit_code == 2, result.output
    assert MISSING in result.output and "Traceback" not in result.output


def test_missing_prior_catalog_is_a_parameter_error(lineage_file, tmp_path):
    result = CliRunner().invoke(
        app,
        [
            "export",
            "catalog",
            str(lineage_file),
            "--out",
            str(tmp_path / "c.json"),
            "--prior",
            MISSING,
        ],
    )
    assert result.exit_code == 2, result.output
    assert MISSING in result.output and "Traceback" not in result.output


def test_missing_describe_catalog_is_a_parameter_error(lineage_file, tmp_path):
    result = CliRunner().invoke(
        app,
        [
            "describe",
            str(lineage_file),
            "--catalog",
            MISSING,
            "--out",
            str(tmp_path / "e.json"),
            "--model",
            "example-model",
        ],
    )
    assert result.exit_code == 2, result.output
    assert MISSING in result.output and "Traceback" not in result.output


def test_missing_run_config_is_a_parameter_error(tmp_path):
    result = CliRunner().invoke(app, ["run", str(tmp_path), "--config", MISSING])
    assert result.exit_code == 2, result.output
    assert MISSING in result.output and "Traceback" not in result.output


def test_export_catalog_creates_missing_output_directories(lineage_file, tmp_path):
    out = tmp_path / "new" / "nested" / "catalog.json"
    result = CliRunner().invoke(app, ["export", "catalog", str(lineage_file), "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert json.loads(out.read_text())["databases"] == []


def test_export_openlineage_creates_missing_output_directories(lineage_file, tmp_path):
    out = tmp_path / "new" / "nested" / "events"
    result = CliRunner().invoke(
        app, ["export", "openlineage", str(lineage_file), "--out", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert out.is_dir()


@pytest.fixture
def sql_fixture(tmp_path):
    """Write one small SQL job to scan.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Path: The written ``.sql`` file.
    """
    source = tmp_path / "job.sql"
    source.write_text("CREATE TABLE b.t AS SELECT x FROM a.s")
    return source


def levels(text):
    """Collect the ``level`` field of every JSON event line in ``text``.

    Args:
        text: Captured stderr.

    Returns:
        set[str]: The distinct levels emitted.
    """
    found = set()
    for line in text.splitlines():
        try:
            found.add(json.loads(line)["level"])
        except (ValueError, KeyError):
            continue
    return found


def test_library_scan_is_quiet_by_default(sql_fixture, capsys):
    from etl_parser.pipeline import scan

    scan(sql_fixture)
    assert "INFO" not in levels(capsys.readouterr().err)


def test_library_scan_honours_an_explicit_level(sql_fixture, capsys):
    from etl_parser.pipeline import scan

    scan(sql_fixture, log_level="INFO")
    assert "INFO" in levels(capsys.readouterr().err)


def test_cli_scan_keeps_info_logging(sql_fixture, tmp_path):
    result = CliRunner().invoke(
        app, ["scan", str(sql_fixture), "--out", str(tmp_path / "lineage.json")]
    )
    assert result.exit_code == 0, result.output
    assert "INFO" in levels(result.stderr)
