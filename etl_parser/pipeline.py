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
from collections.abc import Mapping
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

GATING_UNRESOLVED_KINDS = frozenset({"unsupported_syntax"})
"""Unresolved kinds that make a command exit non-zero.

Only genuinely unparsed code gates. Informational kinds (``analysis_note``,
``skipped_entry``, ``missing_in_source``) stay visible in the JSON summary so CI can gate
on parser coverage without failing on heuristic notes (review finding 23).
"""

PROVIDER_AUTH_REASONS = frozenset(
    {"provider_authentication_failed", "provider_authorization_failed"}
)
"""AI-stage failure reasons that mean the provider rejected our credentials (HTTP 401/403).

These are configuration errors rather than partial results, so they are visible in the exit
code without ``--strict`` (review finding 24).
"""


def blocking_unresolved(document) -> list[Unresolved]:
    """Return the unresolved items that should make a command fail.

    Args:
        document: The :class:`~etl_parser.models.LineageDocument` to inspect.

    Returns:
        list[Unresolved]: Items whose ``kind`` is in :data:`GATING_UNRESOLVED_KINDS`.
    """
    return [issue for issue in document.unresolved if issue.kind in GATING_UNRESOLVED_KINDS]


def provider_authentication_failed(decisions=(), warnings=()) -> bool:
    """Report whether the AI stage failed because the provider rejected our credentials.

    Args:
        decisions: Per-file decision records from an analysis run; each may carry a
            ``reason``.
        warnings: Human-readable warning strings from an analysis run.

    Returns:
        bool: True if any decision reason, or any warning text, names a provider
        authentication or authorization failure.
    """
    for decision in decisions:
        if isinstance(decision, Mapping) and decision.get("reason") in PROVIDER_AUTH_REASONS:
            return True
    return any(
        isinstance(warning, str) and reason in warning
        for warning in warnings
        for reason in PROVIDER_AUTH_REASONS
    )


def exit_code(document, *, decisions=(), warnings=(), status="success", strict=False) -> int:
    """Compute the process exit code for a ``scan`` or ``run`` command.

    Args:
        document: The effective :class:`~etl_parser.models.LineageDocument`.
        decisions: Per-file AI decision records, when an AI stage ran.
        warnings: Warning strings collected during the run.
        status: The observer's final run status.
        strict: Whether ``--strict`` was requested, which also fails for any unresolved
            item, any warning, or a non-success status.

    Returns:
        int: ``2`` for a provider authentication/authorization failure, ``1`` for blocking
        unresolved items or an unmet ``--strict`` requirement, and ``0`` otherwise.
    """
    if provider_authentication_failed(decisions, warnings):
        return 2
    if blocking_unresolved(document):
        return 1
    if strict and (document.unresolved or warnings or status != "success"):
        return 1
    return 0


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
    log_level: str | None = None,
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
        schema: Optional schema provider for qualifying SQL and expanding stars.
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
        log_level: Console log level. Left unset, embedded use stays quiet at
            :data:`~etl_parser.observability.LIBRARY_LOG_LEVEL` (WARNING); the CLI
            passes ``"INFO"``. The log file always retains DEBUG events.
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
