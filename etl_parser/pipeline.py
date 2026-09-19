"""Deterministic scan pipeline: source indexing, parser dispatch, and graph assembly.

Implements the top half of the architecture in spec section 4: it indexes a repository or
GitHub URL into a :class:`~etl_parser.scanner.repo.ScanIndex`, routes each file to the
matching :class:`ParserPlugin` (SQL, Python/pandas/PySpark, or Airflow), collects the
:class:`~etl_parser.models.WorkerResult` each parser returns, and hands the combined
results to :func:`etl_parser.graph.builder.build_graph`. No LLM is used anywhere in this
path (spec section 2, goal 1). The public entry point is :func:`scan`; see
``docs/superpowers/specs/2026-09-15-etl-parser-design.md`` sections 4, 5, and 12.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from etl_parser.graph.builder import build_graph
from etl_parser.identity import dataset_ref_from_id
from etl_parser.models import Job, Unresolved, WorkerResult
from etl_parser.observability import current_observer, digest, observed
from etl_parser.registry import ProductRegistry
from etl_parser.scanner.repo import ScanIndex, SourceFile, imports_airflow
from etl_parser.scanner.sinks import ENGINE_DIALECT
from etl_parser.schema.base import SchemaSource, as_schema_provider, is_schema_source
from etl_parser.sources import GitHubSource, LocalSource, SourceProvider
from etl_parser.workers.airflow import AirflowWorker
from etl_parser.workers.base import comment_schedule, parse_header
from etl_parser.workers.python import PythonWorker
from etl_parser.workers.sql import SchemaProvider, SqlWorker, _sql_description


@dataclass
class ScanContext:
    """Per-scan configuration shared by every :class:`ParserPlugin` invocation.

    Attributes:
        index: The indexed source tree (files and module graph) being scanned.
        schema: Optional schema provider used by :class:`~etl_parser.workers.sql.SqlWorker`
            to qualify columns and expand stars.
        bindings: String substitutions applied to ``{{name}}``/``${name}`` placeholders in
            SQL text before parsing.
        default_db: Database to assume for one-part table names.
        sql_engine: Default execution engine for standalone ``.sql`` files.
        sql_dialect: Default sqlglot dialect for standalone ``.sql`` files.
    """

    index: ScanIndex
    schema: SchemaProvider | None = None
    bindings: dict[str, str] = field(default_factory=dict)
    default_db: str | None = None
    sql_engine: str = "athena"
    sql_dialect: str = "trino"


class ParserPlugin(Protocol):
    """Interface a language/orchestrator parser must implement to plug into the pipeline.

    Plugins return the same :class:`~etl_parser.models.WorkerResult`; graph and export code
    needs no parser-specific branches.

    Attributes:
        name: Unique parser name, used for registration and observability tags.
        extensions: File suffixes this parser might handle; used to widen the set of files
            the scan indexes.
    """

    name: str
    extensions: set[str]

    def accepts(self, source: SourceFile) -> bool:
        """Return whether this parser should analyze ``source``.

        Args:
            source: The indexed file being routed.

        Returns:
            bool: True if this parser claims the file.
        """
        ...

    def analyze(self, source: SourceFile, context: ScanContext) -> WorkerResult:
        """Analyze ``source`` and return its lineage findings.

        Args:
            source: The indexed file to analyze.
            context: Shared scan context (index, schema, bindings, SQL defaults).

        Returns:
            WorkerResult: Datasets, jobs, edges and unresolved items for the file.
        """
        ...


class SqlParser:
    """Parses standalone ``.sql`` files with :class:`~etl_parser.workers.sql.SqlWorker`."""

    name = "sql"
    extensions = {".sql"}

    def accepts(self, source):
        """Return True for any file with a ``.sql`` suffix.

        Args:
            source: The indexed file being routed.
        """
        return source.suffix == ".sql"

    def analyze(self, source, context):
        """Substitute bindings, run :class:`~etl_parser.workers.sql.SqlWorker`, and build
        the file's :class:`~etl_parser.models.Job` (including a comment-declared schedule,
        if present).

        Args:
            source: The ``.sql`` file to analyze.
            context: Shared scan configuration (schema provider, bindings, engine/dialect).

        Returns:
            WorkerResult: The SQL worker's findings plus this file's job appended.
        """
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
    """Parses non-Airflow ``.py`` files with :class:`~etl_parser.workers.python.PythonWorker`."""

    name = "python_frames"
    extensions = {".py"}

    def accepts(self, source):
        """Return True for ``.py`` files that do not import ``airflow``.

        Args:
            source: The indexed file being routed.
        """
        return source.suffix == ".py" and not imports_airflow(source)

    def analyze(self, source, context):
        """Run :class:`~etl_parser.workers.python.PythonWorker` over the file.

        Args:
            source: The ``.py`` file to analyze.
            context: Shared scan configuration (schema provider, bindings, module index).

        Returns:
            WorkerResult: The Python worker's findings for this file.
        """
        return PythonWorker(
            SqlWorker(context.schema),
            context.index,
            bindings=context.bindings,
            default_db=context.default_db,
        ).analyze_source(source)


class AirflowParser:
    """Parses ``.py`` files that import ``airflow`` with
    :class:`~etl_parser.workers.airflow.AirflowWorker`.
    """

    name = "airflow"
    extensions = {".py"}

    def accepts(self, source):
        """Return True for ``.py`` files that import ``airflow``.

        Args:
            source: The indexed file being routed.
        """
        return source.suffix == ".py" and imports_airflow(source)

    def analyze(self, source, context):
        """Run :class:`~etl_parser.workers.airflow.AirflowWorker` over the DAG file.

        Args:
            source: The ``.py`` DAG file to analyze.
            context: Shared scan configuration (schema provider, bindings, module index).

        Returns:
            WorkerResult: The Airflow worker's findings (schedules, tasks, task jobs) for
            this file.
        """
        return AirflowWorker(
            context.index, SqlWorker(context.schema), bindings=context.bindings
        ).analyze_source(source)


class ParserRegistry:
    """The ordered set of :class:`ParserPlugin` instances a scan will try, in order.

    Defaults to SQL, then non-Airflow Python, then Airflow. Callers extend it with
    trusted, explicitly named plugins (CLI ``--plugin module:factory``) to add support for
    other languages or orchestrators without modifying this package.
    """

    def __init__(self, parsers=None):
        """Create a registry.

        Args:
            parsers: Explicit list of parsers to use, replacing the default set. If
                ``None``, defaults to ``[SqlParser(), PythonParser(), AirflowParser()]``.
        """
        self.parsers = (
            list(parsers) if parsers is not None else [SqlParser(), PythonParser(), AirflowParser()]
        )

    def register(self, parser: ParserPlugin):
        """Add a parser to the registry.

        Args:
            parser: The parser plugin to add.

        Raises:
            ValueError: If a parser with the same ``name`` is already registered.
        """
        if any(p.name == parser.name for p in self.parsers):
            raise ValueError(f"Parser already registered: {parser.name}")
        self.parsers.append(parser)


def _alias_hints(schema: SchemaSource, results: list[WorkerResult]) -> list:
    """Turn a schema source's alias hints into dataset refs for identity merging.

    Only hints touching a dataset some worker referenced are used, so the graph gains no
    datasets the code never reads or writes.

    Args:
        schema: The scan's schema source (``aliases()`` is consulted).
        results: Worker results collected so far.

    Returns:
        list[DatasetRef]: One ref per applicable hint, carrying the physical id as an
        alias and ``physical_location``; sorted by logical id.
    """
    referenced = set()
    for result in results:
        referenced.update(d.id for d in result.datasets)
        referenced.update(e.source for e in result.table_edges)
        referenced.update(e.target for e in result.table_edges)
    hints = []
    for logical, physical in sorted(schema.aliases()):
        if logical in referenced or physical in referenced:
            ref = dataset_ref_from_id(logical)
            hints.append(
                ref.model_copy(update={"aliases": [physical], "physical_location": physical})
            )
    return hints


@observed("scan")
def scan(
    path: Path | str,
    *,
    schema: SchemaProvider | SchemaSource | dict | str | Path | None = None,
    include_code_schema: bool = False,
    generate: list[str] | None = None,
    databases: list[str] | None = None,
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
    """Scan a repository or GitHub URL and build its deterministic lineage graph.

    Indexes the source (locally or, for an ``https://`` URL, via the GitHub API without
    cloning), runs every matching :class:`ParserPlugin` over each file, merges the results,
    and builds the :class:`~etl_parser.graph.builder.LineageGraph`. No LLM is used anywhere
    in this path.

    Args:
        path: Local filesystem path, or an ``https://`` GitHub repository URL.
        schema: Optional source-of-truth schema for qualifying SQL and expanding stars: a
            ``SchemaProvider``, a full :class:`~etl_parser.schema.base.SchemaSource`
            (Glue/Postgres/Redshift; its alias hints merge ``glue://`` tables with their
            S3 locations and it is kept on ``graph.schema_source`` for exporters), a
            catalog dict, or a path to a ``catalog.json``/schema mapping file.
        include_code_schema: Recorded on ``graph.export_options`` for the catalog
            exporter: whether code-only tables/columns may be added to ``databases``.
        generate: Recorded on ``graph.export_options``: catalog sections to generate
            (``databases``, ``scripts``, ``relations``, ``lineage``, ``schedules``);
            ``None`` means all.
        databases: Recorded on ``graph.export_options``: database names to restrict the
            catalog to; ``None`` means all.
        bindings: String substitutions for ``{{name}}``/``${name}`` SQL placeholders.
        default_db: Database to assume for one-part table names.
        products: Path to a ``product.yaml`` file or directory of them. If omitted,
            ``product.yaml`` files found while scanning are loaded automatically.
        parsers: Parser registry to use; defaults to a new :class:`ParserRegistry`.
        sql_engine: Default execution engine for standalone ``.sql`` files.
        sql_dialect: Default sqlglot dialect for standalone ``.sql`` files; derived from
            ``sql_engine`` when omitted.
        scan_commit: Commit identifier to stamp on the result; defaults to the source
            provider's detected revision.
        log_dir: Directory to persist run events and metrics in.
        log_level: Console log level; the log file always retains DEBUG events.
        log_max_bytes: Maximum size of a single log file before rotation.
        log_max_files: Maximum number of rotated log files to keep.
        observer: Unused; the active observer is always looked up via
            :func:`~etl_parser.observability.current_observer`.
        source_provider: Explicit source provider; defaults to
            :class:`~etl_parser.sources.GitHubSource` for ``https://`` paths or
            :class:`~etl_parser.sources.LocalSource` otherwise.
        ref: GitHub branch, tag, or commit to pin. Only valid with a GitHub URL.
        source_path: Subpath within the GitHub repository to scan. Only valid with a
            GitHub URL.

    Returns:
        graph.builder.LineageGraph: The built lineage graph, with ``source_index`` set to
        the indexed source tree.

    Raises:
        ValueError: If ``ref`` or ``source_path`` is given for a non-GitHub, non-explicit
            ``path`` (they apply only to GitHub URLs).
    """
    observer = current_observer()
    parsers = parsers or ParserRegistry()
    schema = as_schema_provider(schema)
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
    if is_schema_source(schema):
        results[0].datasets.extend(_alias_hints(schema, results))
    with observer.span("graph.build"):
        graph = build_graph(results, registry, scan_commit)
    graph.schema_source = schema if is_schema_source(schema) else None
    graph.export_options = {
        "include_code_schema": include_code_schema,
        "generate": sorted(generate) if generate is not None else None,
        "databases": sorted(databases) if databases is not None else None,
    }
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
