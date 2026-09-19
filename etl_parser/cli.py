"""Command-line interface. Scanning never imports the scanned repository (spec section 12).

Typer app with commands ``run`` (deterministic lineage plus optional, explicitly enabled AI
work), ``scan`` (deterministic lineage only), ``export catalog``/``export openlineage``,
``impact``, ``products``, and ``describe`` (AI column descriptions). Each command's
docstring is also its ``--help`` text.

Exit codes come from :func:`etl_parser.pipeline.exit_code`: ``1`` when an
``unsupported_syntax`` unresolved item remains (informational kinds never gate) or when
``--strict`` is set on ``run`` and unresolved items, warnings or a non-success run status
remain, and ``2`` for a usage error or an AI stage that failed provider
authentication/authorization. CI can therefore gate on parser coverage without failing on
heuristic notes.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
from enum import StrEnum
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
from etl_parser.pipeline import ParserRegistry, exit_code
from etl_parser.pipeline import scan as scan_repository
from etl_parser.workers.sql import DictSchemaProvider, GlueSchemaProvider


class CatalogSection(StrEnum):
    """A top-level section of the agent ``catalog.json`` that an export can generate.

    Selecting sections narrows what a command rebuilds; unselected sections are passed
    through unchanged from ``--prior`` when one is supplied.
    """

    databases = "databases"
    scripts = "scripts"
    relations = "relations"
    lineage = "lineage"
    schedules = "schedules"


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
    source: str = typer.Argument(..., help="Local path or https://github.com/OWNER/REPO"),
    out_dir: Path = typer.Option(
        Path("artifacts"), help="Directory to write this run's artifact folder into"
    ),
    config: Path | None = typer.Option(
        None, exists=True, help="JSON AnalysisConfig; explicit options override its values"
    ),
    ai_lineage: str | None = typer.Option(None, help="off (default), fallback, or improve"),
    descriptions: bool | None = typer.Option(
        None, "--descriptions/--no-descriptions", help="Generate missing column descriptions"
    ),
    background_comparison: bool | None = typer.Option(
        None,
        "--background-comparison/--no-background-comparison",
        help="Allow a needed description call to also return a shadow lineage comparison",
    ),
    dry_run: bool | None = typer.Option(
        None, "--dry-run/--no-dry-run", help="Plan AI work without making any provider call"
    ),
    schema: Path | None = typer.Option(
        None, exists=True, help="JSON catalog or schema mapping for qualifying SQL and stars"
    ),
    glue: bool = typer.Option(False, help="Fetch input schemas from AWS Glue"),
    plugin: list[str] | None = typer.Option(None, help="Explicitly trusted module:factory plugins"),
    bindings: Path | None = typer.Option(
        None, exists=True, help="JSON string-to-string substitutions for SQL placeholders"
    ),
    products: Path | None = typer.Option(
        None, exists=True, help="product.yaml file, or a directory of them"
    ),
    prior: Path | None = typer.Option(
        None,
        exists=True,
        help="Prior catalog.json whose descriptions and metadata are preserved",
    ),
    engine: str = typer.Option("athena", help="Default execution engine for standalone .sql files"),
    dialect: str | None = typer.Option(
        None, help="Default sqlglot dialect for standalone .sql files"
    ),
    default_db: str | None = typer.Option(None, help="Database to assume for one-part table names"),
    ref: str | None = typer.Option(None, help="GitHub branch, tag or commit to pin"),
    source_path: str | None = typer.Option(None, "--path", help="GitHub repository subpath"),
    lambda_arn: str | None = typer.Option(
        None,
        envvar="ETL_PARSER_LAMBDA_ARN",
        help="Function name or ARN for the Bedrock invoke runner",
    ),
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
    model: str | None = typer.Option(
        None, envvar="ETL_PARSER_MODEL", help="SDK model id or registered model slug"
    ),
    region: str | None = typer.Option(
        None, envvar="AWS_REGION", help="AWS region for the Bedrock invoke runner and Glue"
    ),
    aws_profile: str | None = typer.Option(
        None, envvar="AWS_PROFILE", help="Named AWS profile for the Lambda runner"
    ),
    web_adapter: bool | None = typer.Option(
        None, "--web-adapter/--no-web-adapter", help="Use the SDK Lambda Web Adapter envelope"
    ),
    max_calls: int | None = typer.Option(
        None, min=0, help="Maximum AI calls this run may make (default: 20)"
    ),
    max_output_tokens: int | None = typer.Option(
        None, min=1, help="Maximum output tokens per AI file request (default: 16000)"
    ),
    max_total_tokens: int | None = typer.Option(
        None, min=1, help="Optional cap on cumulative accounted tokens for the run"
    ),
    max_context_chars: int | None = typer.Option(
        None, min=1024, help="Maximum serialized context characters per AI call"
    ),
    timeout_seconds: float | None = typer.Option(
        None, min=0.001, help="Per-request timeout in seconds (default: 300)"
    ),
    deadline_seconds: float | None = typer.Option(
        None, min=0.001, help="Wall-clock budget for the AI stage (default: 3600)"
    ),
    include: list[str] | None = typer.Option(
        None, help="Glob selecting files eligible for AI work; repeatable"
    ),
    exclude: list[str] | None = typer.Option(
        None, help="Glob excluding files from AI work; repeatable, wins over --include"
    ),
    generate: list[CatalogSection] | None = typer.Option(
        None, help="Catalog section to generate; repeatable, defaults to every section"
    ),
    database: list[str] | None = typer.Option(
        None, help="Database name to generate for; repeatable, defaults to every database"
    ),
    min_ai_confidence: float | None = typer.Option(
        None,
        min=0.0,
        max=1.0,
        help="Defer AI proposals whose reported confidence is below this threshold (0-1)",
    ),
    strict: bool = typer.Option(False, help="Fail if any unresolved/AI-incomplete work remains"),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
    log_max_bytes: int = typer.Option(
        10_000_000, min=1024, help="Maximum size of one log file before rotation"
    ),
    log_max_files: int = typer.Option(20, min=1, help="Maximum number of rotated log files"),
):
    """Run deterministic lineage plus explicitly enabled AI work; defaults make no AI calls.

    Args:
        source: Local path or ``https://github.com/OWNER/REPO`` to analyze.
        out_dir: Directory to write result artifacts to.
        config: JSON file of base :class:`~etl_parser.ai_analysis.AnalysisConfig` settings;
            any other option given on the command line overrides its value.
        ai_lineage: ``off`` (default), ``fallback``, or ``improve``.
        descriptions: Whether to also generate column descriptions.
        background_comparison: Whether to run a background AI-vs-static comparison.
        dry_run: Whether to validate configuration without making AI calls.
        schema: JSON schema file for qualifying SQL and expanding stars.
        glue: Fetch input schemas from AWS Glue instead of ``--schema``.
        plugin: Explicitly trusted ``module:factory`` parser plugins to register.
        bindings: JSON file of string substitutions for SQL placeholders.
        products: Path to a ``product.yaml`` file or directory of them.
        prior: Prior catalog JSON to preserve descriptions and flags from.
        generate: Catalog sections to generate; every section when omitted. Sections that
            are not selected are passed through unchanged from ``prior``.
        database: Database names to generate for; every database when omitted.
        engine: Default execution engine for standalone ``.sql`` files.
        dialect: Default sqlglot dialect for standalone ``.sql`` files.
        default_db: Database to assume for one-part table names.
        ref: GitHub branch, tag, or commit to pin (GitHub sources only).
        source_path: Subpath within a GitHub repository to scan.
        lambda_arn: Lambda function name or ARN for the Bedrock invoke runner.
        runner: ``lambda-bedrock-invoke`` (default), ``lbi`` (alias), or ``anthropic``.
        base_url: Anthropic-compatible HTTPS root; SDK appends ``/v1/messages``.
        api_key: Prefer the environment variable over a command-line secret.
        extra_headers: JSON object of custom HTTP headers for the Anthropic runner.
        extra_headers_file: JSON header object file; mutually exclusive with
            ``extra_headers``.
        model: SDK model id or registered model slug for AI work.
        region: AWS region for the Bedrock invoke runner.
        aws_profile: Named AWS profile to use.
        web_adapter: Whether to use the SDK Lambda Web Adapter envelope.
        max_calls: Maximum number of AI calls to make.
        max_output_tokens: Maximum output tokens per AI file request (default: 16000).
        max_total_tokens: Maximum total tokens across all AI calls.
        max_context_chars: Maximum characters of context sent per AI call.
        timeout_seconds: Per-request timeout.
        deadline_seconds: Overall deadline for AI work.
        include: Glob patterns limiting which files AI work considers.
        exclude: Glob patterns excluding files from AI work.
        min_ai_confidence: Minimum model-reported confidence an AI proposal needs to be
            applied; lower proposals are recorded as deferred. Requires an
            :class:`~etl_parser.ai_analysis.AnalysisConfig` that declares the field.
        strict: Fail if any unresolved item, warning, or non-success status remains.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.
        log_max_bytes: Maximum size of a single log file before rotation.
        log_max_files: Maximum number of rotated log files to keep.

    Raises:
        typer.BadParameter: If ``config`` is not a JSON object, the merged configuration
            fails validation, both ``schema`` and ``glue`` are given, ``bindings`` is not a
            JSON object of string keys and values, or ``min_ai_confidence`` is given but
            unsupported by the installed configuration model.
        typer.Exit: With code 1 when a blocking ``unsupported_syntax`` item remains or
            ``strict`` is set and unresolved items, warnings, or a non-success status
            remain, and code 2 when the AI stage failed provider authentication.
    """
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
    if min_ai_confidence is not None:
        if "min_ai_confidence" not in AnalysisConfig.model_fields:
            raise typer.BadParameter(
                "This build's AnalysisConfig has no min_ai_confidence field; "
                "upgrade etl-parser to filter AI proposals by confidence",
                param_hint="--min-ai-confidence",
            )
        settings["min_ai_confidence"] = min_ai_confidence
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
        **_selection(generate, database, analyze),
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
    code = exit_code(
        result.document,
        decisions=result.decisions,
        warnings=result.warnings,
        status=current_observer().status,
        strict=strict,
    )
    if code:
        raise typer.Exit(code)


def _selection(generate, databases, target=None):
    """Build the catalog-selection keyword arguments ``target`` accepts.

    ``generate``/``databases`` are keywords of
    :func:`~etl_parser.export.agent_catalog.export_agent_catalog`. A callable that does
    not name both of them gets neither, so the flags stay inert rather than raising on a
    build whose exporter, analysis or scan entry point has not learned them yet.

    Args:
        generate: Selected :class:`CatalogSection` values, or ``None`` for every section.
        databases: Selected database names, or ``None`` for every database.
        target: The callable the keywords would be passed to; defaults to
            :func:`~etl_parser.export.agent_catalog.export_agent_catalog`.

    Returns:
        dict: ``{"generate": [...], "databases": [...]}`` when ``target`` names both
        parameters, otherwise an empty mapping. ``None`` values mean "everything".
    """
    named = {
        name
        for name, parameter in inspect.signature(target or export_agent_catalog).parameters.items()
        if parameter.kind is not parameter.VAR_KEYWORD
    }
    if not {"generate", "databases"} <= named:
        return {}
    return {
        "generate": [section.value for section in generate] if generate else None,
        "databases": list(databases) if databases else None,
    }


def _json(path):
    """Read and parse a JSON file.

    Args:
        path: Path to the JSON file.

    Returns:
        The parsed JSON value.
    """
    return json.loads(path.read_text(encoding="utf-8"))


def _headers(raw, path):
    """Resolve and validate extra HTTP headers from an inline JSON string or a file.

    Args:
        raw: Inline JSON object string of headers, or ``None``.
        path: Path to a JSON header object file, or ``None``.

    Returns:
        dict[str, str] | None: The validated headers, or ``None`` if neither ``raw`` nor
        ``path`` was given.

    Raises:
        typer.BadParameter: If both ``raw`` and ``path`` are given, or the resolved value
            is not valid JSON, is not readable, or fails
            :class:`~etl_parser.describe.client.RunnerConfig` header validation.
    """
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
    """Serialize a value to deterministic, sorted JSON and write it to disk.

    Creates the destination's parent directories when they do not exist, so an export can
    write straight into a new results folder (review finding 35).

    Args:
        value: The JSON-serializable value to write.
        path: Destination file path.
    """
    text = json.dumps(value, sort_keys=True, indent=2) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
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
    """Import and call a ``module:factory`` spec to build a trusted parser plugin.

    Args:
        spec: A ``"module:factory"`` string naming a zero-argument callable that returns a
            :class:`~etl_parser.pipeline.ParserPlugin`.

    Returns:
        ParserPlugin: The result of calling the resolved factory.

    Raises:
        typer.BadParameter: If ``spec`` has no ``:`` separator.
        ModuleNotFoundError: If ``module`` cannot be imported.
        AttributeError: If ``module`` has no attribute ``attribute``.
    """
    module, separator, attribute = spec.partition(":")
    if not separator:
        raise typer.BadParameter("Expected module:factory")
    return getattr(importlib.import_module(module), attribute)()


@app.command()
@observed("command.scan")
def scan(
    repo: str = typer.Argument(..., help="Local path or https://github.com/OWNER/REPO"),
    out: Path = typer.Option(
        Path("lineage.json"), help="Destination for the native lineage JSON output"
    ),
    schema: Path | None = typer.Option(
        None, exists=True, help="JSON catalog or schema mapping for qualifying SQL and stars"
    ),
    glue: bool = typer.Option(False, help="Fetch input schemas from AWS Glue"),
    region: str | None = typer.Option(None, help="AWS region for the Glue schema lookup"),
    products: Path | None = typer.Option(
        None, exists=True, help="product.yaml file, or a directory of them"
    ),
    bindings: Path | None = typer.Option(
        None, exists=True, help="JSON string-to-string substitutions for SQL placeholders"
    ),
    default_db: str | None = typer.Option(None, help="Database to assume for one-part table names"),
    engine: str = typer.Option("athena", help="Default execution engine for standalone .sql files"),
    dialect: str | None = typer.Option(
        None, help="Default sqlglot dialect for standalone .sql files"
    ),
    plugin: list[str] | None = typer.Option(None, help="Explicitly trusted module:factory plugins"),
    generate: list[CatalogSection] | None = typer.Option(
        None, help="Catalog section to generate; repeatable, defaults to every section"
    ),
    database: list[str] | None = typer.Option(
        None, help="Database name to generate for; repeatable, defaults to every database"
    ),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
    log_max_bytes: int = typer.Option(
        10_000_000, min=1024, help="Maximum size of one log file before rotation"
    ),
    log_max_files: int = typer.Option(20, min=1, help="Maximum number of rotated log files"),
    ref: str | None = typer.Option(None, help="GitHub branch, tag or commit to pin"),
    source_path: str | None = typer.Option(None, "--path", help="GitHub repository subpath"),
):
    """Scan a repo, directory, Python/SQL file, or ZIP library into native JSON.

    Args:
        repo: Local path or ``https://github.com/OWNER/REPO``.
        out: Destination for the native ``lineage.json`` output.
        schema: JSON schema file for qualifying SQL and expanding stars.
        glue: Fetch input schemas from AWS Glue instead of ``--schema``.
        region: AWS region for the Glue schema provider.
        products: Path to a ``product.yaml`` file or directory of them.
        bindings: JSON file of string substitutions for SQL placeholders.
        default_db: Database to assume for one-part table names.
        engine: Default execution engine for standalone ``.sql`` files.
        dialect: Default sqlglot dialect for standalone ``.sql`` files.
        plugin: Explicitly trusted ``module:factory`` parser plugins to register.
        generate: Catalog sections a later export should generate; every section when
            omitted. Recorded for the export step, which owns the catalog sections.
        database: Database names a later export should generate for; every database when
            omitted.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.
        log_max_bytes: Maximum size of a single log file before rotation.
        log_max_files: Maximum number of rotated log files to keep.
        ref: GitHub branch, tag, or commit to pin.
        source_path: Subpath within the GitHub repository to scan.

    Raises:
        typer.BadParameter: If both ``schema`` and ``glue`` are given, or ``bindings`` is
            not a JSON object of string keys and values.
        typer.Exit: With code 1 if any unresolved item has kind ``unsupported_syntax``.
    """
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
        **_selection(generate, database, scan_repository),
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
    if exit_code(doc):
        raise typer.Exit(1)


@export_app.command("catalog")
@observed("command.export_catalog")
def catalog_export(
    lineage: Path = typer.Argument(
        ..., exists=True, dir_okay=False, help="Saved native lineage.json to export"
    ),
    out: Path = typer.Option(..., help="Destination catalog.json; parent directories are created"),
    prior: Path | None = typer.Option(
        None,
        exists=True,
        dir_okay=False,
        help="Prior catalog.json whose descriptions and metadata are preserved",
    ),
    generate: list[CatalogSection] | None = typer.Option(
        None, help="Catalog section to generate; repeatable, defaults to every section"
    ),
    database: list[str] | None = typer.Option(
        None, help="Database name to generate for; repeatable, defaults to every database"
    ),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
):
    """Create the agent catalog, retaining prior descriptions and flags.

    Args:
        lineage: Path to a native ``lineage.json`` file.
        out: Destination for the generated ``catalog.json``.
        prior: Prior ``catalog.json`` to merge into, preserving human-only fields.
        generate: Catalog sections to generate; every section when omitted. Sections that
            are not selected are passed through unchanged from ``prior``.
        database: Database names to generate for; every database when omitted.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.

    Raises:
        typer.BadParameter: If ``lineage`` or ``prior`` does not exist; the message names
            the missing path.
    """
    _write(
        export_agent_catalog(
            read_native(lineage),
            _json(prior) if prior else None,
            **_selection(generate, database),
        ),
        out,
    )


@export_app.command("openlineage")
@observed("command.export_openlineage")
def openlineage_export(
    lineage: Path = typer.Argument(
        ..., exists=True, dir_okay=False, help="Saved native lineage.json to export"
    ),
    out: Path = typer.Option(..., help="Directory for one event file per job; created if absent"),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
):
    """Write one synthetic static OpenLineage event per job.

    Args:
        lineage: Path to a native ``lineage.json`` file.
        out: Directory to write one ``<runId>.json`` event file per job into; created
            with its parents when absent.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.

    Raises:
        typer.BadParameter: If ``lineage`` does not exist; the message names the path.
    """
    out.mkdir(parents=True, exist_ok=True)
    for event in export_openlineage(read_native(lineage)):
        _write(event, out / f"{event['run']['runId']}.json")


@app.command()
@observed("command.impact")
def impact(
    lineage: Path = typer.Argument(
        ..., exists=True, dir_okay=False, help="Saved native lineage.json to query"
    ),
    node: str = typer.Argument(..., help="Dataset id, or dataset_id#column id, to query"),
    upstream_direction: bool = typer.Option(
        False, "--upstream", help="Walk upstream provenance instead of downstream consumers"
    ),
    depth: int | None = typer.Option(
        None, min=0, help="Maximum hop distance to include; unbounded when omitted"
    ),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
):
    """Query a dataset ID or dataset#column ID.

    Args:
        lineage: Path to a native ``lineage.json`` file.
        node: Dataset id or ``dataset_id#column`` id to query.
        upstream_direction: Walk upstream (provenance) instead of downstream (consumers).
        depth: Maximum hop distance to include; unbounded if omitted.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.

    Raises:
        typer.BadParameter: If ``lineage`` does not exist, ``depth`` is negative, or
            ``node`` is not a known dataset or column id.
    """
    graph = LineageGraph(read_native(lineage))
    try:
        report = (upstream if upstream_direction else downstream)(graph, node, depth)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(report, indent=2, sort_keys=True))


@app.command()
@observed("command.products")
def products(
    lineage: Path = typer.Argument(
        ..., exists=True, dir_okay=False, help="Saved native lineage.json to report on"
    ),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
):
    """Report product dependency and orchestrator drift.

    Args:
        lineage: Path to a native ``lineage.json`` file.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.

    Raises:
        typer.BadParameter: If ``lineage`` does not exist; the message names the path.
    """
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
    lineage: Path = typer.Argument(
        ..., exists=True, dir_okay=False, help="Saved native lineage.json describing the columns"
    ),
    catalog: Path = typer.Option(
        ..., exists=True, dir_okay=False, help="Existing catalog.json to enrich with descriptions"
    ),
    out: Path = typer.Option(
        ..., help="Destination for the enriched catalog; parent directories are created"
    ),
    lambda_arn: str | None = typer.Option(
        None,
        envvar="ETL_PARSER_LAMBDA_ARN",
        help="Function name or ARN for the Bedrock invoke runner",
    ),
    runner: str = typer.Option(
        "lambda-bedrock-invoke",
        envvar="ETL_PARSER_RUNNER",
        help="lambda-bedrock-invoke, lbi (alias), or anthropic",
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
    model: str = typer.Option(
        ..., envvar="ETL_PARSER_MODEL", help="SDK model id or registered model slug"
    ),
    region: str = typer.Option(
        "us-east-1", envvar="AWS_REGION", help="AWS region for the Bedrock invoke runner"
    ),
    aws_profile: str | None = typer.Option(
        None, envvar="AWS_PROFILE", help="Named AWS profile for the Lambda runner"
    ),
    web_adapter: bool = typer.Option(True, help="Use the SDK Lambda Web Adapter envelope"),
    max_tokens: int = typer.Option(
        16000, min=1, help="Maximum output tokens per description request"
    ),
    log_dir: Path | None = typer.Option(None, help="Persist run events and metrics in this folder"),
    log_level: str = typer.Option("INFO", help="Console level; file logs retain DEBUG events"),
    log_max_bytes: int = typer.Option(
        10_000_000, min=1024, help="Maximum size of one log file before rotation"
    ),
    log_max_files: int = typer.Option(20, min=1, help="Maximum number of rotated log files"),
):
    """Generate descriptions through the selected Agent SDK runner (paid calls).

    Args:
        lineage: Path to a native ``lineage.json`` file.
        catalog: Existing ``catalog.json`` to enrich with descriptions.
        out: Destination for the enriched catalog.
        lambda_arn: Lambda function name or ARN for the Bedrock invoke runner.
        runner: ``lambda-bedrock-invoke``, ``lbi`` (alias), or ``anthropic``.
        base_url: Anthropic-compatible HTTPS root; SDK appends ``/v1/messages``.
        api_key: Prefer the environment variable over a command-line secret.
        extra_headers: JSON object of custom HTTP headers for the Anthropic runner.
        extra_headers_file: JSON header object file; mutually exclusive with
            ``extra_headers``.
        model: SDK model id or registered model slug.
        region: AWS region for the Bedrock invoke runner.
        aws_profile: Named AWS profile to use.
        web_adapter: Use the SDK Lambda Web Adapter envelope.
        max_tokens: Maximum output tokens per description request.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; file logs always retain DEBUG events.
        log_max_bytes: Maximum size of a single log file before rotation.
        log_max_files: Maximum number of rotated log files to keep.

    Raises:
        typer.BadParameter: If ``lineage`` or ``catalog`` does not exist, the runner
            configuration is invalid (bad runner/URL/headers combination), or runner
            initialization fails (missing SDK, missing credentials, or invalid engine
            construction).
    """
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
        """Construct the configured runner, run the description engine, and close it."""
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
