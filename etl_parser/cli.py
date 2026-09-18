"""Command-line interface. Scanning never imports the scanned repository."""

from __future__ import annotations

import asyncio
import importlib
import json
from pathlib import Path

import typer

from etl_parser.describe.client import RunnerConfig, close_runner, configured_runner
from etl_parser.describe.engine import DescriptionEngine
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.export.native import read_native, write_native
from etl_parser.export.openlineage_out import export_openlineage
from etl_parser.graph.builder import LineageGraph
from etl_parser.graph.impact import downstream, upstream
from etl_parser.graph.products import orchestration_drift, product_dependencies
from etl_parser.observability import current_observer, digest, observed
from etl_parser.pipeline import ParserRegistry
from etl_parser.pipeline import scan as scan_repository
from etl_parser.workers.sql import DictSchemaProvider, GlueSchemaProvider

app = typer.Typer(
    no_args_is_help=True,
    help="Static ETL lineage with optional audited AI analysis. "
    "Use COMMAND --help for all options; scanning does not run your ETL code.",
    pretty_exceptions_show_locals=False,
)
export_app = typer.Typer(
    no_args_is_help=True, help="Export saved native lineage to catalog or OpenLineage."
)
app.add_typer(export_app, name="export")


@app.command("run")
@observed("command.run")
def analysis_run(
    source: str,
    out_dir: Path = typer.Option(Path("artifacts")),
    config: Path | None = typer.Option(None, exists=True),
    ai_lineage: str | None = typer.Option(None, help="off (default), fallback, or improve"),
    descriptions: bool | None = typer.Option(None, "--descriptions/--no-descriptions"),
    background_comparison: bool | None = typer.Option(
        None, "--background-comparison/--no-background-comparison"
    ),
    dry_run: bool | None = typer.Option(None, "--dry-run/--no-dry-run"),
    schema: Path | None = typer.Option(None, exists=True),
    glue: bool = typer.Option(False, help="Fetch input schemas from AWS Glue"),
    plugin: list[str] | None = typer.Option(None, help="Explicitly trusted module:factory plugins"),
    bindings: Path | None = typer.Option(None, exists=True),
    products: Path | None = typer.Option(None, exists=True),
    prior: Path | None = typer.Option(None, exists=True),
    engine: str = "athena",
    dialect: str | None = None,
    default_db: str | None = None,
    ref: str | None = None,
    source_path: str | None = typer.Option(None, "--path"),
    lambda_arn: str | None = typer.Option(None, envvar="ETL_PARSER_LAMBDA_ARN"),
    runner: str | None = typer.Option(
        None,
        envvar="ETL_PARSER_RUNNER",
        help="lambda-bedrock-invoke (default), lbi (alias), or anthropic",
    ),
    base_url: str | None = typer.Option(
        None,
        envvar=["ANTHROPIC_API_BASE_URL", "ANTHROPIC_BASE_URL"],
        help="Anthropic-compatible HTTPS root; SDK appends /v1/messages",
    ),
    api_key: str | None = typer.Option(
        None,
        envvar="ANTHROPIC_API_KEY",
        help="Prefer the environment variable over a command-line secret",
    ),
    extra_headers: str | None = typer.Option(None, help="JSON object of custom HTTP headers"),
    extra_headers_file: Path | None = typer.Option(
        None, exists=True, help="JSON header object; mutually exclusive with --extra-headers"
    ),
    model: str | None = typer.Option(None, envvar="ETL_PARSER_MODEL"),
    region: str | None = typer.Option(None, envvar="AWS_REGION"),
    aws_profile: str | None = typer.Option(None, envvar="AWS_PROFILE"),
    web_adapter: bool | None = typer.Option(None, "--web-adapter/--no-web-adapter"),
    max_calls: int | None = typer.Option(None, min=0),
    max_output_tokens: int | None = typer.Option(
        None, min=1, help="Maximum output tokens per AI file request (default: 16000)"
    ),
    max_total_tokens: int | None = typer.Option(None, min=1),
    max_context_chars: int | None = typer.Option(None, min=1024),
    timeout_seconds: float | None = typer.Option(None, min=0.001),
    deadline_seconds: float | None = typer.Option(None, min=0.001),
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    strict: bool = typer.Option(False, help="Fail if any unresolved/AI-incomplete work remains"),
    log_dir: Path | None = None,
    log_level: str = "INFO",
    log_max_bytes: int = typer.Option(10_000_000, min=1024),
    log_max_files: int = typer.Option(20, min=1),
):
    """Run deterministic lineage plus explicitly enabled AI work; defaults make no AI calls."""
    from etl_parser.ai_analysis import AnalysisConfig, analyze
    from etl_parser.artifacts import write_analysis

    settings = _json(config) if config else {}
    if not isinstance(settings, dict):
        raise typer.BadParameter("Config must be a JSON object")
    settings.update(
        {
            k: v
            for k, v in {
                "ai_lineage": ai_lineage,
                "descriptions": descriptions,
                "background_comparison": background_comparison,
                "dry_run": dry_run,
                "lambda_arn": lambda_arn,
                "runner": runner,
                "base_url": base_url,
                "api_key": api_key,
                "extra_headers": _headers(extra_headers, extra_headers_file),
                "model": model,
                "region": region,
                "aws_profile": aws_profile,
                "web_adapter": web_adapter,
                "max_calls": max_calls,
                "max_output_tokens": max_output_tokens,
                "max_total_tokens": max_total_tokens,
                "max_context_chars": max_context_chars,
                "timeout_seconds": timeout_seconds,
                "deadline_seconds": deadline_seconds,
                "include": include,
                "exclude": exclude,
            }.items()
            if v is not None
        }
    )
    try:
        options = AnalysisConfig.model_validate(settings)
    except ValueError as exc:
        raise typer.BadParameter(
            "Invalid analysis configuration; check option types and bounds"
        ) from exc
    values = _json(bindings) if bindings else {}
    if schema and glue:
        raise typer.BadParameter("Choose either --schema or --glue")
    registry = ParserRegistry()
    for spec in plugin or []:
        registry.register(_factory(spec))
    if not isinstance(values, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in values.items()
    ):
        raise typer.BadParameter("Bindings must map string names to string values")
    result = analyze(
        source,
        config=options,
        prior=_json(prior) if prior else None,
        schema=DictSchemaProvider(schema)
        if schema
        else GlueSchemaProvider(region=region)
        if glue
        else None,
        parsers=registry,
        bindings=values,
        products=products,
        sql_engine=engine,
        sql_dialect=dialect,
        default_db=default_db,
        ref=ref,
        source_path=source_path,
    )
    folder = write_analysis(result, out_dir)
    typer.echo(
        json.dumps(
            {
                "output": str(folder),
                "jobs": len(result.document.jobs),
                "ai_lineage": options.ai_lineage,
                "descriptions": options.descriptions,
                "dry_run": options.dry_run,
                "warnings": len(result.warnings),
                "status": current_observer().status,
            },
            sort_keys=True,
        )
    )
    if strict and (
        result.document.unresolved or result.warnings or current_observer().status != "success"
    ):
        raise typer.Exit(1)


def _json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _headers(raw, path):
    if raw is not None and path is not None:
        raise typer.BadParameter("Choose --extra-headers or --extra-headers-file, not both")
    if raw is None and path is None:
        return None
    try:
        value = json.loads(raw) if raw is not None else _json(path)
        return RunnerConfig(extra_headers=value).extra_headers
    except (ValueError, OSError):
        raise typer.BadParameter(
            "Extra headers must be a valid JSON object of safe string headers"
        ) from None


def _write(value, path):
    text = json.dumps(value, sort_keys=True, indent=2) + "\n"
    path.write_text(text, encoding="utf-8")
    observer = current_observer()
    if observer:
        observer.count("artifacts.written")
        observer.count("artifacts.bytes", len(text.encode("utf-8")))
        observer.event(
            "artifact.written",
            path=str(path),
            size_bytes=len(text.encode("utf-8")),
            content_digest=digest(text),
        )


def _factory(spec):
    module, separator, attribute = spec.partition(":")
    if not separator:
        raise typer.BadParameter("Expected module:factory")
    return getattr(importlib.import_module(module), attribute)()


@app.command()
@observed("command.scan")
def scan(
    repo: str = typer.Argument(..., help="Local path or https://github.com/OWNER/REPO"),
    out: Path = typer.Option(Path("lineage.json")),
    schema: Path | None = typer.Option(None, exists=True),
    glue: bool = typer.Option(False, help="Fetch input schemas from AWS Glue"),
    region: str | None = None,
    products: Path | None = typer.Option(None, exists=True),
    bindings: Path | None = typer.Option(None, exists=True),
    default_db: str | None = None,
    engine: str = "athena",
    dialect: str | None = None,
    plugin: list[str] | None = typer.Option(None, help="Explicitly trusted module:factory plugins"),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
    log_max_bytes: int = typer.Option(10_000_000, min=1024),
    log_max_files: int = typer.Option(20, min=1),
    ref: str | None = typer.Option(None, help="GitHub branch, tag or commit to pin"),
    source_path: str | None = typer.Option(None, "--path", help="GitHub repository subpath"),
):
    """Scan a repo, directory, Python/SQL file, or ZIP library into native JSON."""
    registry = ParserRegistry()
    if schema and glue:
        raise typer.BadParameter("Choose either --schema or --glue")
    for spec in plugin or []:
        registry.register(_factory(spec))
    values = _json(bindings) if bindings else {}
    if not isinstance(values, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in values.items()
    ):
        raise typer.BadParameter("Bindings must be a JSON object of string keys and values")
    graph = scan_repository(
        repo,
        schema=DictSchemaProvider(schema)
        if schema
        else GlueSchemaProvider(region=region)
        if glue
        else None,
        bindings=values,
        products=products,
        default_db=default_db,
        parsers=registry,
        sql_engine=engine,
        sql_dialect=dialect,
        ref=ref,
        source_path=source_path,
    )
    doc = graph.to_document()
    write_native(doc, out)
    typer.echo(
        json.dumps(
            {
                "output": str(out),
                "jobs": len(doc.jobs),
                "datasets": len(doc.datasets),
                "unresolved": len(doc.unresolved),
                **doc.summary(),
            },
            sort_keys=True,
        )
    )
    if any(issue.kind == "unsupported_syntax" for issue in doc.unresolved):
        raise typer.Exit(1)


@export_app.command("catalog")
@observed("command.export_catalog")
def catalog_export(
    lineage: Path,
    out: Path = typer.Option(...),
    prior: Path | None = None,
    log_dir: Path | None = None,
    log_level: str = "INFO",
):
    """Create the agent catalog, retaining prior descriptions and flags."""
    _write(export_agent_catalog(read_native(lineage), _json(prior) if prior else None), out)


@export_app.command("openlineage")
@observed("command.export_openlineage")
def openlineage_export(
    lineage: Path,
    out: Path = typer.Option(...),
    log_dir: Path | None = None,
    log_level: str = "INFO",
):
    """Write one synthetic static OpenLineage event per job."""
    out.mkdir(parents=True, exist_ok=True)
    for event in export_openlineage(read_native(lineage)):
        _write(event, out / f"{event['run']['runId']}.json")


@app.command()
@observed("command.impact")
def impact(
    lineage: Path,
    node: str,
    upstream_direction: bool = typer.Option(False, "--upstream"),
    depth: int | None = typer.Option(None, min=0),
    log_dir: Path | None = None,
    log_level: str = "INFO",
):
    """Query a dataset ID or dataset#column ID."""
    graph = LineageGraph(read_native(lineage))
    try:
        report = (upstream if upstream_direction else downstream)(graph, node, depth)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(report, indent=2, sort_keys=True))


@app.command()
@observed("command.products")
def products(lineage: Path, log_dir: Path | None = None, log_level: str = "INFO"):
    """Report product dependency and orchestrator drift."""
    graph = LineageGraph(read_native(lineage))
    typer.echo(
        json.dumps(
            {
                "dependencies": product_dependencies(graph),
                "orchestration_drift": orchestration_drift(graph),
            },
            indent=2,
            sort_keys=True,
        )
    )


@app.command()
@observed("command.describe")
def describe(
    lineage: Path,
    catalog: Path = typer.Option(...),
    out: Path = typer.Option(...),
    lambda_arn: str | None = typer.Option(None, envvar="ETL_PARSER_LAMBDA_ARN"),
    runner: str = typer.Option(
        "lambda-bedrock-invoke",
        envvar="ETL_PARSER_RUNNER",
        help="lambda-bedrock-invoke, lbi (alias), or anthropic",
    ),
    base_url: str | None = typer.Option(
        None, envvar=["ANTHROPIC_API_BASE_URL", "ANTHROPIC_BASE_URL"]
    ),
    api_key: str | None = typer.Option(None, envvar="ANTHROPIC_API_KEY"),
    extra_headers: str | None = typer.Option(None, help="JSON object of custom HTTP headers"),
    extra_headers_file: Path | None = typer.Option(None, exists=True),
    model: str = typer.Option(..., envvar="ETL_PARSER_MODEL"),
    region: str = typer.Option("us-east-1", envvar="AWS_REGION"),
    aws_profile: str | None = typer.Option(None, envvar="AWS_PROFILE"),
    web_adapter: bool = typer.Option(True, help="Use the SDK Lambda Web Adapter envelope"),
    max_tokens: int = typer.Option(16000, min=1),
    log_dir: Path | None = None,
    log_level: str = "INFO",
    log_max_bytes: int = typer.Option(10_000_000, min=1024),
    log_max_files: int = typer.Option(20, min=1),
):
    """Generate descriptions through the selected Agent SDK runner (paid calls)."""
    doc, existing = read_native(lineage), _json(catalog)
    try:
        options = RunnerConfig(
            runner=runner,
            lambda_arn=lambda_arn,
            region=region,
            aws_profile=aws_profile,
            web_adapter=web_adapter,
            base_url=base_url,
            api_key=api_key,
            extra_headers=_headers(extra_headers, extra_headers_file) or {},
        )
    except ValueError:
        raise typer.BadParameter(
            "Invalid runner configuration; check runner, URL and headers"
        ) from None
    if options.api_key:
        current_observer().protect(options.api_key.get_secret_value())
    current_observer().protect(*options.extra_headers.values())
    current_observer().configure(
        **options.model_dump(),
        model=model,
        ai_lineage="off",
        descriptions=True,
    )
    current_observer().event(
        "description.configured",
        actor="orchestrator",
        runner=runner,
        model=model,
        lambda_arn=lambda_arn,
        region=region,
        aws_profile=aws_profile,
        web_adapter=web_adapter,
        max_output_tokens=max_tokens,
        ai_lineage="off",
    )

    async def generate():
        selected = configured_runner(options)
        try:
            engine = DescriptionEngine(selected, model=model, max_tokens=max_tokens)
            return await engine.arun(doc, existing), engine.warnings
        finally:
            await close_runner(selected)

    try:
        enriched, warnings = asyncio.run(generate())
    except (RuntimeError, ValueError, ImportError):
        raise typer.BadParameter(
            "Runner initialization failed; check SDK and selected provider settings"
        ) from None
    _write(enriched, out)
    current_observer().event("description.completed", warning_count=len(warnings))
