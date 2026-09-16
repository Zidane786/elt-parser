"""Command-line interface. Scanning never imports the scanned repository."""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import typer

from etl_parser.describe.engine import DescriptionEngine
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.export.native import read_native, write_native
from etl_parser.export.openlineage_out import export_openlineage
from etl_parser.graph.builder import LineageGraph
from etl_parser.graph.impact import downstream, upstream
from etl_parser.graph.products import orchestration_drift, product_dependencies
from etl_parser.pipeline import ParserRegistry
from etl_parser.pipeline import scan as scan_repository
from etl_parser.workers.sql import DictSchemaProvider, GlueSchemaProvider

app = typer.Typer(no_args_is_help=True)
export_app = typer.Typer(no_args_is_help=True)
app.add_typer(export_app, name="export")


def _json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write(value, path):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _factory(spec):
    module, separator, attribute = spec.partition(":")
    if not separator:
        raise typer.BadParameter("Expected module:factory")
    return getattr(importlib.import_module(module), attribute)()


@app.command()
def scan(
    repo: Path = typer.Argument(..., exists=True),
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
def catalog_export(lineage: Path, out: Path = typer.Option(...), prior: Path | None = None):
    """Create the agent catalog, retaining prior descriptions and flags."""
    _write(export_agent_catalog(read_native(lineage), _json(prior) if prior else None), out)


@export_app.command("openlineage")
def openlineage_export(lineage: Path, out: Path = typer.Option(...)):
    """Write one synthetic static OpenLineage event per job."""
    out.mkdir(parents=True, exist_ok=True)
    for event in export_openlineage(read_native(lineage)):
        _write(event, out / f"{event['run']['runId']}.json")


@app.command()
def impact(
    lineage: Path,
    node: str,
    upstream_direction: bool = typer.Option(False, "--upstream"),
    depth: int | None = typer.Option(None, min=0),
):
    """Query a dataset ID or dataset#column ID."""
    graph = LineageGraph(read_native(lineage))
    try:
        report = (upstream if upstream_direction else downstream)(graph, node, depth)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps(report, indent=2, sort_keys=True))


@app.command()
def products(lineage: Path):
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
def describe(
    lineage: Path,
    catalog: Path = typer.Option(...),
    client: str = typer.Option(...),
    out: Path = typer.Option(...),
):
    """Enrich descriptions using an explicitly chosen client; may call its external provider."""
    engine = DescriptionEngine(_factory(client))
    _write(engine.run(read_native(lineage), _json(catalog)), out)
    for warning in engine.warnings:
        typer.echo(warning, err=True)
