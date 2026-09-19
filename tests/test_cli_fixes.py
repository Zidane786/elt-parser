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


def parameters():
    """Walk every command in the app and yield its documented parameters.

    Yields:
        tuple[str, str, object]: Command path, parameter label, and the parameter.
    """
    from typer.main import get_command

    def walk(command, path):
        """Recurse into ``command`` and its subcommands."""
        for parameter in command.params:
            label = next(
                (option for option in getattr(parameter, "opts", []) if option.startswith("--")),
                parameter.name,
            )
            yield path, label, parameter
        for name, child in getattr(command, "commands", {}).items():
            yield from walk(child, f"{path} {name}".strip())

    yield from walk(get_command(app), "etl-parser")


def test_every_command_parameter_has_a_help_string():
    missing = sorted(
        f"{path} {label}"
        for path, label, parameter in parameters()
        if not getattr(parameter, "help", None)
    )
    assert not missing, f"Undocumented parameters: {missing}"


@pytest.fixture
def captured_config(monkeypatch):
    """Record the :class:`AnalysisConfig` ``run`` builds, without analyzing anything.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        list: One-element list that receives the configuration ``run`` passes on.
    """
    seen = []

    class Result:
        """Empty analysis result."""

        document = document()
        decisions = ()
        warnings = ()

    def analyze(source, *, config, **kwargs):
        """Capture the resolved configuration."""
        seen.append(config)
        return Result()

    monkeypatch.setattr("etl_parser.ai_analysis.analyze", analyze)
    monkeypatch.setattr("etl_parser.artifacts.write_analysis", lambda result, out: Path(out))
    return seen


def with_confidence_field(monkeypatch):
    """Install an :class:`AnalysisConfig` that already has ``min_ai_confidence`` (WP-E)."""
    from etl_parser.ai_analysis import AnalysisConfig

    class Configured(AnalysisConfig):
        """Configuration extended with the WP-E confidence threshold."""

        min_ai_confidence: float = 0.0

    monkeypatch.setattr("etl_parser.ai_analysis.AnalysisConfig", Configured)


def test_min_ai_confidence_reaches_the_analysis_configuration(
    captured_config, monkeypatch, tmp_path
):
    with_confidence_field(monkeypatch)
    result = CliRunner().invoke(
        app,
        ["run", str(tmp_path), "--out-dir", str(tmp_path / "out"), "--min-ai-confidence", "0.75"],
    )
    assert result.exit_code == 0, result.output
    assert captured_config[0].min_ai_confidence == 0.75


def test_min_ai_confidence_is_a_no_op_when_unset(captured_config, tmp_path):
    result = CliRunner().invoke(app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out")])
    assert result.exit_code == 0, result.output
    assert not hasattr(captured_config[0], "min_ai_confidence")


def test_min_ai_confidence_reports_a_clear_error_when_unsupported(captured_config, tmp_path):
    result = CliRunner().invoke(
        app,
        ["run", str(tmp_path), "--out-dir", str(tmp_path / "out"), "--min-ai-confidence", "0.5"],
    )
    assert result.exit_code == 2, result.output
    assert "min-ai-confidence" in result.output and "Traceback" not in result.output


def test_min_ai_confidence_rejects_values_outside_zero_to_one(tmp_path):
    result = CliRunner().invoke(
        app, ["run", str(tmp_path), "--out-dir", str(tmp_path / "out"), "--min-ai-confidence", "2"]
    )
    assert result.exit_code == 2, result.output


GUIDES = ("README.md", "docs/cli.md", "docs/sdk.md", "docs/dependencies.md")
# Spelled in parts so this file does not itself contain the string it forbids.
INTERNAL_HEADER = "x-" + "duke"
PRIVATE_DISTRIBUTION = "gd" + "tc"


def guide_text():
    """Read every user-facing guide.

    Returns:
        dict[str, str]: Guide path to its text.
    """
    return {name: (ROOT / name).read_text() for name in GUIDES}


@pytest.mark.parametrize("name", GUIDES)
def test_guides_use_neutral_example_header_names(name):
    text = (ROOT / name).read_text()
    assert INTERNAL_HEADER not in text, f"{name} names an internal gateway header"


@pytest.mark.parametrize("name", GUIDES)
def test_guides_refer_to_the_private_sdk_generically(name):
    text = (ROOT / name).read_text()
    assert PRIVATE_DISTRIBUTION not in text.lower(), f"{name} names the private distribution"


@pytest.mark.parametrize("name", GUIDES)
def test_guides_keep_the_internal_index_generic(name):
    text = (ROOT / name).read_text()
    assert "nexus" not in text.lower(), f"{name} names an internal package index"


def test_header_examples_are_placeholders_everywhere_in_the_tree():
    offenders = [
        str(path.relative_to(ROOT))
        for path in list((ROOT / "etl_parser").rglob("*.py")) + list((ROOT / "tests").glob("*.py"))
        if INTERNAL_HEADER in path.read_text()
    ]
    assert not offenders, f"Internal header names remain in: {offenders}"


@pytest.mark.parametrize(
    "topic",
    [
        "--schema-from-code",
        "missing_in_source",
        "schema_drift",
        "relations_inferred",
        "description_source",
        "ai_confidence",
        "--min-ai-confidence",
        "--generate",
        "--database",
        "etl-parser schema fetch",
        "ETL_PARSER_POSTGRES_DSN",
        "ETL_PARSER_REDSHIFT_DSN",
    ],
)
def test_cli_guide_documents_the_new_behaviour(topic):
    assert topic in (ROOT / "docs" / "cli.md").read_text(), f"Missing from cli.md: {topic}"


@pytest.mark.parametrize(
    "topic",
    [
        "include_code_schema",
        "fetch_schema",
        "GlueSchemaSource",
        "PostgresSchemaSource",
        "RedshiftSchemaSource",
        "SchemaSourceError",
        "write_schema_catalog",
        "schema_drift",
        "relations_inferred",
        "description_source",
        "ai_rationale",
        "min_ai_confidence",
        'AnalysisPolicyError("provider_configuration_invalid")',
    ],
)
def test_sdk_guide_documents_the_new_surface(topic):
    assert topic in (ROOT / "docs" / "sdk.md").read_text(), f"Missing from sdk.md: {topic}"


@pytest.mark.parametrize(
    "topic", ["--generate", "--database", "Exit codes", "relations_inferred", "schema_drift"]
)
def test_readme_documents_the_new_behaviour(topic):
    assert topic in (ROOT / "README.md").read_text(), f"Missing from README: {topic}"


def test_guides_document_the_exit_code_table():
    for name in ("README.md", "docs/cli.md"):
        text = (ROOT / name).read_text()
        assert "analysis_note" in text and "unsupported_syntax" in text, name


def test_credentials_are_documented_as_environment_only():
    for name in ("docs/cli.md", "docs/sdk.md"):
        text = (ROOT / name).read_text()
        assert "--password" in text or "no `--dsn`" in text or "no `dsn=`" in text, name


def test_dependency_guide_documents_every_declared_requirement():
    text = (ROOT / "docs" / "dependencies.md").read_text()
    for section in (
        "Runtime requirements",
        "Optional extras",
        "Development tools",
        "Considered and not used",
    ):
        assert section in text, f"Missing section: {section}"
    declared = project()
    for requirement in declared["dependencies"]:
        assert f"`{requirement}`" in text, f"Undocumented runtime dependency: {requirement}"
    for extra, requirements in declared["optional-dependencies"].items():
        assert f"`{extra}`" in text
        for requirement in requirements:
            assert f"`{requirement}`" in text, f"Undocumented extra dependency: {requirement}"
    for dropped in ("grimp", "libcst", "jedi", "sqllineage", "sqlglotc", "astroid", "anthropic"):
        assert f"`{dropped}`" in text, f"Missing rationale for dropping {dropped}"
    assert "internal package index" in text


def test_readme_links_the_dependency_guide():
    assert "docs/dependencies.md" in (ROOT / "README.md").read_text()


def project():
    """Parse ``pyproject.toml``.

    Returns:
        dict: The parsed project table.
    """
    import tomllib

    return tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]


def test_unused_dependencies_are_not_declared():
    declared = {name.split(">")[0].split("[")[0].strip() for name in project()["dependencies"]}
    assert "astroid" not in declared
    assert "anthropic" not in declared


def test_runtime_dependencies_are_the_ones_actually_imported():
    declared = {name.split(">")[0].split("[")[0].strip() for name in project()["dependencies"]}
    assert declared == {
        "pydantic",
        "sqlglot",
        "networkx",
        "pyyaml",
        "typer",
        "openlineage-python",
    }


def test_optional_extras_cover_every_schema_source():
    extras = project()["optional-dependencies"]
    assert set(extras) == {"glue", "postgres", "redshift"}
    assert any(name.startswith("boto3") for name in extras["glue"])
    assert any(name.startswith("psycopg") for name in extras["postgres"])
    assert any(name.startswith("redshift_connector") for name in extras["redshift"])


def test_optional_drivers_are_never_imported_at_module_level():
    import re

    pattern = re.compile(r"^(?:import|from)\s+(?:boto3|psycopg|redshift_connector)\b", re.M)
    offenders = [
        str(path.relative_to(ROOT))
        for path in (ROOT / "etl_parser").rglob("*.py")
        if pattern.search(path.read_text())
    ]
    assert not offenders, f"Optional drivers imported at module level: {offenders}"


@pytest.fixture
def exporter(monkeypatch):
    """Replace the catalog exporter with one that records its keyword arguments.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        Callable[..., list]: Installs an exporter that either accepts the selection
        keywords or has the older two-argument signature, and returns its call records.
    """

    def install(selective=True):
        """Install the exporter and return the list its calls are recorded in."""
        calls = []

        if selective:

            def export(doc, prior=None, *, generate=None, databases=None):
                """Record a selective export."""
                calls.append({"generate": generate, "databases": databases})
                return {"databases": [], "relations": []}

        else:

            def export(doc, prior=None):
                """Record an export that predates the selection keywords."""
                calls.append({})
                return {"databases": [], "relations": []}

        monkeypatch.setattr("etl_parser.cli.export_agent_catalog", export)
        return calls

    return install


def export_arguments(lineage, out, *extra):
    """Build an ``export catalog`` argument list.

    Args:
        lineage: Saved lineage file.
        out: Destination catalog path.
        *extra: Extra command-line arguments.

    Returns:
        list[str]: The full argument list.
    """
    return ["export", "catalog", str(lineage), "--out", str(out), *extra]


def test_export_catalog_passes_selected_sections_and_databases(exporter, lineage_file, tmp_path):
    calls = exporter()
    result = CliRunner().invoke(
        app,
        export_arguments(
            lineage_file,
            tmp_path / "c.json",
            "--generate",
            "scripts",
            "--generate",
            "databases",
            "--database",
            "analytics",
        ),
    )
    assert result.exit_code == 0, result.output
    assert calls == [{"generate": ["scripts", "databases"], "databases": ["analytics"]}]


def test_export_catalog_generates_everything_by_default(exporter, lineage_file, tmp_path):
    calls = exporter()
    result = CliRunner().invoke(app, export_arguments(lineage_file, tmp_path / "c.json"))
    assert result.exit_code == 0, result.output
    assert calls == [{"generate": None, "databases": None}]


def test_export_catalog_works_with_an_exporter_without_selection_support(
    exporter, lineage_file, tmp_path
):
    calls = exporter(selective=False)
    result = CliRunner().invoke(
        app, export_arguments(lineage_file, tmp_path / "c.json", "--generate", "scripts")
    )
    assert result.exit_code == 0, result.output
    assert calls == [{}]


@pytest.mark.parametrize("command", [["export", "catalog"], ["run"], ["scan"]])
def test_generate_rejects_unknown_section_names(command, lineage_file, tmp_path):
    target = str(lineage_file) if command[0] == "export" else str(tmp_path)
    result = CliRunner().invoke(app, [*command, target, "--generate", "bogus"])
    assert result.exit_code == 2, result.output
    assert "bogus" in result.output


@pytest.mark.parametrize("section", ["databases", "scripts", "relations", "lineage", "schedules"])
def test_every_documented_section_name_is_accepted(section, exporter, lineage_file, tmp_path):
    calls = exporter()
    result = CliRunner().invoke(
        app, export_arguments(lineage_file, tmp_path / "c.json", "--generate", section)
    )
    assert result.exit_code == 0, result.output
    assert calls[0]["generate"] == [section]


def test_run_accepts_the_selection_flags(analyzed, tmp_path):
    analyzed(document())
    result = CliRunner().invoke(
        app,
        [
            "run",
            str(tmp_path),
            "--out-dir",
            str(tmp_path / "out"),
            "--generate",
            "scripts",
            "--database",
            "analytics",
        ],
    )
    assert result.exit_code == 0, result.output


def test_scan_accepts_the_selection_flags(scanned, tmp_path):
    scanned(document())
    result = CliRunner().invoke(
        app,
        [
            "scan",
            str(tmp_path),
            "--out",
            str(tmp_path / "lineage.json"),
            "--generate",
            "scripts",
            "--database",
            "analytics",
        ],
    )
    assert result.exit_code == 0, result.output
