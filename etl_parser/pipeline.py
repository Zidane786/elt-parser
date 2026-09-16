"""Public scan API and explicit parser/orchestrator extension registry."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from etl_parser.graph.builder import build_graph
from etl_parser.models import Job, Unresolved, WorkerResult
from etl_parser.registry import ProductRegistry
from etl_parser.scanner.repo import RepoScanner, ScanIndex, SourceFile, imports_airflow
from etl_parser.scanner.sinks import ENGINE_DIALECT
from etl_parser.workers.airflow import AirflowWorker
from etl_parser.workers.base import comment_schedule, parse_header
from etl_parser.workers.python import PythonWorker
from etl_parser.workers.sql import SchemaProvider, SqlWorker, _sql_description


@dataclass
class ScanContext:
    index: ScanIndex
    schema: SchemaProvider | None = None
    bindings: dict[str, str] = field(default_factory=dict)
    default_db: str | None = None
    sql_engine: str = "athena"
    sql_dialect: str = "trino"


class ParserPlugin(Protocol):
    """Plugins return the same WorkerResult; graph/export code needs no parser branches."""

    name: str
    extensions: set[str]

    def accepts(self, source: SourceFile) -> bool: ...
    def analyze(self, source: SourceFile, context: ScanContext) -> WorkerResult: ...


class SqlParser:
    name = "sql"
    extensions = {".sql"}

    def accepts(self, source):
        return source.suffix == ".sql"

    def analyze(self, source, context):
        worker = SqlWorker(context.schema)
        sql = re.sub(
            r"\{\{\s*([A-Za-z_]\w*)\s*\}\}|\$\{([A-Za-z_]\w*)\}",
            lambda match: context.bindings.get(match.group(1) or match.group(2), match.group()),
            source.text,
        )
        analysis = worker.analyze(
            sql,
            job_id=source.job_id,
            engine=context.sql_engine,
            dialect=context.sql_dialect,
            default_db=context.default_db,
            source_file=source.path,
        )
        header = parse_header(source.text)
        job = Job(
            id=source.job_id,
            name=Path(source.path).stem,
            source_file=source.path,
            language="sql",
            engine=context.sql_engine,
            dialect=context.sql_dialect,
            description=_sql_description(source.text),
            owner=header.get("owner"),
            inputs=sorted(analysis.inputs),
            outputs=sorted(analysis.outputs),
        )
        if "schedule" in header:
            schedule = comment_schedule(job.id, header["schedule"], source.path)
            analysis.result.schedules[schedule.id] = schedule
            job.schedule_id = schedule.id
        analysis.result.jobs.append(job)
        return analysis.result


class PythonParser:
    name = "python_frames"
    extensions = {".py"}

    def accepts(self, source):
        return source.suffix == ".py" and not imports_airflow(source)

    def analyze(self, source, context):
        return PythonWorker(
            SqlWorker(context.schema),
            context.index,
            bindings=context.bindings,
            default_db=context.default_db,
        ).analyze_source(source)


class AirflowParser:
    name = "airflow"
    extensions = {".py"}

    def accepts(self, source):
        return source.suffix == ".py" and imports_airflow(source)

    def analyze(self, source, context):
        return AirflowWorker(
            context.index, SqlWorker(context.schema), bindings=context.bindings
        ).analyze_source(source)


class ParserRegistry:
    def __init__(self, parsers=None):
        self.parsers = (
            list(parsers) if parsers is not None else [SqlParser(), PythonParser(), AirflowParser()]
        )

    def register(self, parser: ParserPlugin):
        if any(p.name == parser.name for p in self.parsers):
            raise ValueError(f"Parser already registered: {parser.name}")
        self.parsers.append(parser)


def scan(
    path: Path | str,
    *,
    schema: SchemaProvider | None = None,
    bindings: dict[str, str] | None = None,
    default_db: str | None = None,
    products: Path | str | None = None,
    parsers: ParserRegistry | None = None,
    sql_engine: str = "athena",
    sql_dialect: str | None = None,
    scan_commit: str | None = None,
):
    parsers = parsers or ParserRegistry()
    extensions = {".yaml", ".yml"} | set().union(*(p.extensions for p in parsers.parsers))
    index = RepoScanner(path, extensions=extensions).scan()
    context = ScanContext(
        index,
        schema,
        bindings or {},
        default_db,
        sql_engine,
        sql_dialect or ENGINE_DIALECT.get(sql_engine, sql_engine),
    )
    results = [WorkerResult(unresolved=index.unresolved)]
    registry = ProductRegistry.load(products) if products else ProductRegistry()
    for source in index.files:
        if source.archive and Path(path).suffix != ".zip":
            continue  # ZIPs are helper libraries, analyzed at import call sites.
        if Path(source.path).name == "product.yaml" and not products:
            try:
                registry.add_text(source.text, source.path)
            except Exception as exc:
                results[0].unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=source.path,
                        reason=f"product metadata: {exc}",
                    )
                )
        if Path(path).is_file() and Path(path).suffix != ".zip" and source.path != Path(path).name:
            continue
        for parser in parsers.parsers:
            try:
                if parser.accepts(source):
                    results.append(parser.analyze(source, context))
            except Exception as exc:
                results[0].unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=source.path,
                        reason=f"{parser.name}: {type(exc).__name__}: {exc}",
                    )
                )
    return build_graph(results, registry, scan_commit)
