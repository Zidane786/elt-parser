"""Public scan API and explicit parser/orchestrator extension registry."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from etl_parser.graph.builder import build_graph
from etl_parser.models import Job, Unresolved, WorkerResult
from etl_parser.observability import current_observer, digest, observed
from etl_parser.registry import ProductRegistry
from etl_parser.scanner.repo import ScanIndex, SourceFile, imports_airflow
from etl_parser.scanner.sinks import ENGINE_DIALECT
from etl_parser.sources import GitHubSource, LocalSource, SourceProvider
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


@observed("scan")
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
    log_dir: Path | str | None = None,
    log_level: str = "INFO",
    log_max_bytes: int = 10_000_000,
    log_max_files: int = 20,
    observer=None,
    source_provider: SourceProvider | None = None,
    ref: str | None = None,
    source_path: str | None = None,
):
    observer = current_observer()
    parsers = parsers or ParserRegistry()
    observer.configure(
        source=str(path),
        parsers=[p.name for p in parsers.parsers],
        sql_engine=sql_engine,
        sql_dialect=sql_dialect,
        scan_commit=scan_commit,
        schema_enabled=schema is not None,
        bindings_count=len(bindings or {}),
        ai_lineage="off",
        descriptions=False,
    )
    observer.event(
        "scan.configured",
        actor="orchestrator",
        source=str(path),
        parsers=[p.name for p in parsers.parsers],
        sql_engine=sql_engine,
        sql_dialect=sql_dialect,
        schema_enabled=schema is not None,
        bindings_count=len(bindings or {}),
        ai_lineage="off",
        descriptions=False,
        scan_commit=scan_commit,
    )
    extensions = {".yaml", ".yml"} | set().union(*(p.extensions for p in parsers.parsers))
    if source_provider is None:
        if str(path).startswith("https://"):
            source_provider = GitHubSource(str(path), ref=ref, path=source_path)
        else:
            if ref or source_path:
                raise ValueError("ref/source_path options apply only to GitHub URLs")
            source_provider = LocalSource(path)
    with observer.span("source.index"):
        index = source_provider.scan(extensions=extensions)
    scan_commit = scan_commit or index.revision
    observer.configure(source_origin=index.origin, scan_commit=scan_commit)
    observer.gauge("source.indexed_files", len(index.files))
    observer.gauge("source.indexed_bytes", sum(len(f.text.encode("utf-8")) for f in index.files))
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
        if source.archive and not index.archive_entries:
            observer.count("files.helper_only")
            observer.event(
                "file.deferred",
                level="DEBUG",
                source=source.path,
                reason="archive_helper_not_entry",
            )
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
        if index.entry_paths is not None and source.path not in index.entry_paths:
            observer.count("files.helper_only")
            continue
        matched = False
        for parser in parsers.parsers:
            try:
                if parser.accepts(source):
                    matched = True
                    observer.count("parser.invocations")
                    with observer.span(
                        f"parser.{parser.name}",
                        source=source.path,
                        source_digest=digest(source.text),
                    ):
                        parsed = parser.analyze(source, context)
                    results.append(parsed)
                    observer.count("parser.completed")
                    for job in parsed.jobs:
                        observer.event(
                            "job.found",
                            level="DEBUG",
                            actor=parser.name,
                            job_id=job.id,
                            source=job.source_file,
                            inputs=job.inputs,
                            outputs=job.outputs,
                        )
                    for edge in parsed.table_edges:
                        observer.count("findings.table_edges")
                        observer.event(
                            "lineage.table_found",
                            level="DEBUG",
                            actor=parser.name,
                            job_id=edge.job_id,
                            source=edge.source,
                            target=edge.target,
                            confidence=edge.provenance.confidence,
                        )
                    for edge in parsed.column_edges:
                        observer.count("findings.column_edges")
                        observer.event(
                            "lineage.column_found",
                            level="DEBUG",
                            actor=parser.name,
                            job_id=edge.job_id,
                            target=edge.target.model_dump(),
                            sources=[r.model_dump() for r in edge.sources],
                            indirect_sources=[r.model_dump() for r in edge.indirect_sources],
                            confidence=edge.provenance.confidence,
                            expression_digest=digest(edge.transformation.expression or ""),
                        )
            except Exception as exc:
                observer.count("parser.failures")
                observer.event(
                    "parser.failed",
                    level="ERROR",
                    actor=parser.name,
                    source=source.path,
                    error_type=type(exc).__name__,
                )
                results[0].unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=source.path,
                        reason=f"{parser.name}: {type(exc).__name__}: {exc}",
                    )
                )
        observer.count("files.parsed" if matched else "files.no_matching_parser")
    with observer.span("graph.build"):
        graph = build_graph(results, registry, scan_commit)
    doc = graph.document
    for category in ("jobs", "datasets", "table_edges", "column_edges", "unresolved"):
        observer.gauge(f"lineage.{category}", len(getattr(doc, category)))
    for confidence, count in doc.summary()["column_edges_by_confidence"].items():
        observer.gauge(f"lineage.confidence.{confidence}", count)
    for issue in doc.unresolved:
        observer.count(f"diagnostics.{issue.kind}")
        observer.event(
            "diagnostic.found",
            level="WARNING",
            source=issue.source_file,
            line=issue.line,
            job_id=issue.job_id,
            kind=issue.kind,
            reason_digest=digest(issue.reason),
        )
    if doc.unresolved:
        observer.partial()
    observer.event(
        "scan.completed",
        jobs=len(doc.jobs),
        datasets=len(doc.datasets),
        table_edges=len(doc.table_edges),
        column_edges=len(doc.column_edges),
        unresolved=len(doc.unresolved),
    )
    graph.source_index = index
    return graph
