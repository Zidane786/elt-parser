"""Pydantic models shared by every worker, the graph builder, and the exporters.

See docs/superpowers/specs/2026-09-15-etl-parser-design.md section 6.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

DatasetKind = Literal["table", "s3_path", "api", "kafka", "kinesis", "file", "unknown"]
TransformationKind = Literal[
    "identity", "expression", "aggregation", "filter", "join", "window", "unknown"
]
Parser = Literal[
    "sqlglot", "python_ast", "pandas_chain", "spark_static", "openlineage_runtime", "airflow"
]
Confidence = Literal["exact", "inferred", "partial"]
Language = Literal["sql", "python", "pyspark", "unknown"]
Engine = Literal["athena", "spark", "postgres", "mysql", "pandas", "polars", "unknown"]
UnresolvedKind = Literal[
    "dynamic_sql",
    "dynamic_table_name",
    "dynamic_path",
    "unsupported_syntax",
    "unknown_column",
    "missing_schema",
    "unresolved_import",
    "dynamic_schedule",
    "external_job",
    "analysis_note",
    "skipped_entry",
]
Origin = Literal["parser", "ai", "runtime", "prior", "registry"]
Orchestrator = Literal["airflow", "product_yaml", "cron_comment"]
DependencySource = Literal["data", "dag"]


class _Model(BaseModel):
    """Base pydantic model shared by every lineage type: forbids unknown fields.

    ``extra="forbid"`` catches typos and drifted call sites at validation time instead of
    silently dropping fields.
    """

    model_config = ConfigDict(extra="forbid")


class DatasetRef(_Model):
    """A dataset node in the lineage graph, identified by its canonical id (section 7).

    Attributes:
        id: Canonical id, ``scheme://namespace/name`` (see ``etl_parser.identity``).
        namespace: The ``scheme://namespace`` portion, e.g. a Glue database or S3 bucket.
        name: The table/object name within the namespace.
        kind: The physical storage kind of the dataset.
        aliases: Other ids (e.g. an S3 path) known to resolve to this same dataset.
        physical_location: The underlying storage location, when known (e.g. an S3 URI for
            a Glue table).
        product: Owning product code, attached from the product registry when it matches a
            declared database.
        layer: Product-declared layer (e.g. raw/stage/mart) for the owning database.
        columns: Column names observed for this dataset across all jobs.
        origin: Who introduced this dataset: ``parser`` (deterministic workers), ``ai``
            (accepted AI proposal), ``runtime`` (OpenLineage events), ``prior`` (carried
            from a prior catalog) or ``registry`` (product YAML).
        provenance: Provenance of the introducing evidence when ``origin`` is not ``parser``.
    """

    id: str
    namespace: str
    name: str
    kind: DatasetKind = "table"
    aliases: list[str] = Field(default_factory=list)
    physical_location: str | None = None
    product: str | None = None
    layer: str | None = None
    columns: list[str] = Field(default_factory=list)
    origin: Origin = "parser"
    provenance: Provenance | None = None


class ColumnRef(_Model):
    """A single column of a dataset, as referenced by an edge endpoint.

    Attributes:
        dataset_id: Canonical id of the owning dataset.
        name: Column name.
        datatype: Column datatype, when known from a schema provider.
    """

    dataset_id: str
    name: str
    datatype: str | None = None


class Transformation(_Model):
    """The code that produced a column or table edge.

    Attributes:
        expression: The SQL or Python source text of the transformation, verbatim.
        kind: The shape of the transformation, used by exporters to pick an OpenLineage
            subtype and by the description engine to decide whether an LLM call is needed.
        source_file: File the expression was read from.
        line_start: First source line of the expression.
        line_end: Last source line of the expression.
    """

    expression: str | None = None
    kind: TransformationKind = "unknown"
    source_file: str | None = None
    line_start: int | None = None
    line_end: int | None = None


class Provenance(_Model):
    """How an edge was derived and how much it can be trusted (section 13).

    Attributes:
        parser: Name of the worker/parser that produced the edge.
        confidence: ``exact`` (fully resolved), ``inferred`` (heuristic applied), or
            ``partial`` (some sources known to be missing).
        dialect: SQL dialect used to parse the expression, when applicable.
        scan_commit: Commit the scan ran against, stamped in by the graph builder.
        model_id: LLM model id, set only on AI-derived provenance.
        request_id: LLM request id, set only on AI-derived provenance.
        evidence_digest: Digest of the evidence an AI-derived edge was grounded in.
        ai_confidence: The model's own 0-1 self-assessment for an AI-derived edge, so a
            consumer can threshold AI output independently of ``confidence``.
        ai_rationale: Short model-supplied reason behind ``ai_confidence``.
    """

    parser: str
    confidence: Confidence = "exact"
    dialect: str | None = None
    scan_commit: str | None = None
    model_id: str | None = None
    request_id: str | None = None
    evidence_digest: str | None = None
    ai_confidence: float | None = Field(default=None, ge=0, le=1)
    ai_rationale: str | None = Field(default=None, max_length=500)


class ColumnEdge(_Model):
    """A column-level lineage edge: one target column derived from its source columns.

    Attributes:
        target: The output column produced by ``job_id``.
        sources: Columns the target's value is directly computed from.
        indirect_sources: Columns that influenced the target without appearing in its
            projection (``WHERE``, ``JOIN ON``, ``GROUP BY``, ``HAVING``), matching
            OpenLineage's indirect transformation types.
        transformation: The expression that computed the target column.
        provenance: How this edge was derived and its confidence.
        job_id: Id of the job that produced this edge.
    """

    target: ColumnRef
    sources: list[ColumnRef] = Field(default_factory=list)
    indirect_sources: list[ColumnRef] = Field(default_factory=list)
    transformation: Transformation = Field(default_factory=Transformation)
    provenance: Provenance
    job_id: str


class TableEdge(_Model):
    """A table-level lineage edge, used when a job reads/writes a dataset but its columns
    could not be resolved (e.g. a star without a schema).

    Attributes:
        source: Canonical id of the dataset read.
        target: Canonical id of the dataset written.
        provenance: How this edge was derived and its confidence.
        job_id: Id of the job that produced this edge.
        source_file: File the read/write call site was found in.
        line: Line of the read/write call site.
        transformation: The expression associated with the read/write, when available.
    """

    source: str
    target: str
    provenance: Provenance
    job_id: str
    source_file: str | None = None
    line: int | None = None
    transformation: Transformation = Field(default_factory=Transformation)


class Schedule(_Model):
    """A schedule declaration for a job or DAG task (section 6).

    A job that appears in several DAGs has several ``Schedule`` entries; ``Job.schedule_id``
    points at the one from the product's primary orchestrator.

    Attributes:
        id: ``<dag_id>.<task_id>`` for Airflow tasks, or a synthetic id for
            ``product.yaml``/comment-declared schedules.
        orchestrator: Where the schedule was declared.
        dag_id: Owning DAG id, for Airflow-derived schedules.
        task_id: Task id within the DAG, or ``None`` for the DAG's own schedule.
        cron: Normalized five-field cron string, or ``None`` when not representable as cron.
        interval_text: The raw declared value (``"@daily"``, ``"0 4 * * *"``, a
            ``timedelta`` repr, or a timetable class name).
        timezone: Declared timezone, when known.
        start_date: Declared start date, when known.
        catchup: Declared catchup flag, when known.
        owner: Declared owner, when known.
        tags: Declared tags.
        declared_upstream: Task/schedule ids this schedule is declared to run after.
        declared_downstream: Task/schedule ids declared to run after this one.
        source_file: File the schedule was declared in.
        line: Line the schedule was declared at.
    """

    id: str
    orchestrator: str
    dag_id: str | None = None
    task_id: str | None = None
    cron: str | None = None
    interval_text: str | None = None
    timezone: str | None = None
    start_date: str | None = None
    catchup: bool | None = None
    owner: str | None = None
    tags: list[str] = Field(default_factory=list)
    declared_upstream: list[str] = Field(default_factory=list)
    declared_downstream: list[str] = Field(default_factory=list)
    source_file: str | None = None
    line: int | None = None


class Job(_Model):
    """A unit of ETL work discovered by a worker: one script, statement, or task.

    Attributes:
        id: Stable job id, unique within a scan.
        name: Human-readable name, typically the source file stem.
        source_file: Path of the file this job was parsed from.
        language: Implementation language.
        engine: Execution engine, e.g. ``athena``, ``spark``, ``pandas``.
        dialect: SQL dialect used by this job, when applicable.
        schedule_id: Key into ``LineageDocument.schedules``/``WorkerResult.schedules`` for
            this job's primary schedule.
        product: Owning product code, attached by the graph builder.
        owner: Declared owner, when known.
        description: Human or AI-authored description; empty until filled downstream.
        inputs: Canonical ids of datasets this job reads.
        outputs: Canonical ids of datasets this job writes.
        origin: ``parser`` for jobs found by deterministic workers, ``ai`` when an accepted
            AI proposal introduced the job.
        ai_inputs: Dataset ids added to ``inputs`` by accepted AI proposals, kept separate so
            deterministic inputs stay distinguishable.
        ai_outputs: Dataset ids added to ``outputs`` by accepted AI proposals.
    """

    id: str
    name: str
    source_file: str
    language: Language = "unknown"
    engine: str = "unknown"
    dialect: str | None = None
    schedule_id: str | None = None
    product: str | None = None
    owner: str | None = None
    description: str | None = None
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    origin: Origin = "parser"
    ai_inputs: list[str] = Field(default_factory=list)
    ai_outputs: list[str] = Field(default_factory=list)


class JobDependency(_Model):
    """One upstream job dependency of a job, with the evidence that established it.

    Attributes:
        job_id: Id of the upstream job.
        sources: Evidence kinds that support this dependency: ``data`` (an output of the
            upstream job is an input of the downstream job) and/or ``dag`` (the
            orchestrator declares this order).
        via_datasets: Datasets that connect the two jobs, when ``data`` is a source.
        in_place_writer: True when the upstream job reads and writes the same dataset (an
            in-place writer), so its true position relative to other readers of that
            dataset can only be established by a ``dag`` edge.
    """

    job_id: str
    sources: list[DependencySource] = Field(default_factory=list)
    via_datasets: list[str] = Field(default_factory=list)
    in_place_writer: bool = False


class Unresolved(_Model):
    """A first-class record of something static analysis could not determine (section 13).

    Workers never raise on user code; every failure becomes one of these instead.

    Attributes:
        kind: The category of what could not be resolved.
        source_file: File where the issue was found.
        line: Line where the issue was found.
        reason: Human-readable explanation.
        partial_text: The reconstructed text with unresolved parts left as placeholders.
        job_id: Job the issue is associated with, when known.
        expression: The offending expression, when applicable.
        symbols: Names that could not be resolved.
        assumptions: Any assumptions made while trying to resolve the item.
        remediation: A suggestion for how to make this resolvable.
    """

    kind: UnresolvedKind
    source_file: str | None = None
    line: int | None = None
    reason: str
    partial_text: str | None = None
    job_id: str | None = None
    expression: str | None = None
    symbols: list[str] = Field(default_factory=list)
    assumptions: dict[str, str] = Field(default_factory=dict)
    remediation: str | None = None


class ProductDatabase(_Model):
    """A database owned by a product, as declared in that product's ``product.yaml``.

    Attributes:
        name: Database name, matched against ``DatasetRef.namespace`` contents.
        type: Declared engine/scheme for the database.
        layer: Declared layer (e.g. raw/stage/mart).
        description: Declared description.
    """

    name: str
    type: str | None = None
    layer: str | None = None
    description: str | None = None


class DeclaredDependency(_Model):
    """A cross-product dependency declared in a product's ``product.yaml``.

    Attributes:
        product: The dependency's product name as written in the YAML, before resolution.
        code: The dependency's product code; resolved from ``product`` by
            ``ProductRegistry.resolve_dependencies``.
        tables: Tables declared to be consumed from the dependency.
    """

    product: str | None = None
    code: str
    tables: list[str] = Field(default_factory=list)


class Product(_Model):
    """A data product as declared in a ``product.yaml`` file.

    Attributes:
        code: Unique product code.
        name: Product display name.
        domain: Declared business domain.
        owners: Declared owners.
        databases: Databases this product owns.
        declared_dependencies: Other products this one declares it depends on.
        orchestrator_type: Declared orchestrator type (e.g. ``airflow``).
        orchestrator_file: Path to the orchestrator's primary DAG file, when declared.
        source_file: Path of the ``product.yaml`` this was parsed from.
    """

    code: str
    name: str
    domain: str | None = None
    owners: list[str] = Field(default_factory=list)
    databases: list[ProductDatabase] = Field(default_factory=list)
    declared_dependencies: list[DeclaredDependency] = Field(default_factory=list)
    orchestrator_type: str | None = None
    orchestrator_file: str | None = None
    source_file: str | None = None


class WorkerResult(_Model):
    """The uniform output every worker returns, merged by the graph builder (section 4).

    Attributes:
        datasets: Datasets observed by this worker.
        jobs: Jobs this worker produced.
        schedules: Schedules found, keyed by ``Schedule.id``.
        task_jobs: Orchestrator task id to job id mapping (``None`` when the task has no
            resolvable job, e.g. an unrecognized operator).
        column_edges: Column-level lineage edges found.
        table_edges: Table-level lineage edges found.
        unresolved: Items that could not be statically resolved.
    """

    datasets: list[DatasetRef] = Field(default_factory=list)
    jobs: list[Job] = Field(default_factory=list)
    schedules: dict[str, Schedule] = Field(default_factory=dict)
    task_jobs: dict[str, str | None] = Field(default_factory=dict)
    column_edges: list[ColumnEdge] = Field(default_factory=list)
    table_edges: list[TableEdge] = Field(default_factory=list)
    unresolved: list[Unresolved] = Field(default_factory=list)

    def extend(self, other: WorkerResult) -> WorkerResult:
        """Merge another result's contents into this one in place.

        Args:
            other: Result to merge in. Its lists are appended and its dicts update this
                result's dicts (later values win on key collisions).

        Returns:
            WorkerResult: This instance, for chaining.
        """
        self.datasets.extend(other.datasets)
        self.jobs.extend(other.jobs)
        self.schedules.update(other.schedules)
        self.task_jobs.update(other.task_jobs)
        self.column_edges.extend(other.column_edges)
        self.table_edges.extend(other.table_edges)
        self.unresolved.extend(other.unresolved)
        return self


class LineageDocument(_Model):
    """The native lineage export: the full graph in one deterministic document.

    Written as ``lineage.json`` by ``etl_parser.export.native.write_native`` and consumed
    by every downstream exporter, the impact analyzer, and the description engine.

    Attributes:
        version: Document schema version.
        generated_at: Timestamp the document was generated, if stamped.
        scan_commit: Commit the scan ran against.
        products: Products loaded from the product registry.
        datasets: All datasets observed across the scan, identity-merged.
        jobs: All jobs found across the scan.
        schedules: All schedules, keyed by ``Schedule.id``.
        task_jobs: Orchestrator task id to job id mapping.
        job_dependencies: For each job id, its resolved upstream dependencies.
        column_edges: All column-level lineage edges.
        table_edges: All table-level lineage edges.
        unresolved: All items that could not be statically resolved.
    """

    version: str = "1"
    generated_at: datetime | None = None
    scan_commit: str | None = None
    products: list[Product] = Field(default_factory=list)
    datasets: list[DatasetRef] = Field(default_factory=list)
    jobs: list[Job] = Field(default_factory=list)
    schedules: dict[str, Schedule] = Field(default_factory=dict)
    task_jobs: dict[str, str | None] = Field(default_factory=dict)
    job_dependencies: dict[str, list[JobDependency]] = Field(default_factory=dict)
    column_edges: list[ColumnEdge] = Field(default_factory=list)
    table_edges: list[TableEdge] = Field(default_factory=list)
    unresolved: list[Unresolved] = Field(default_factory=list)

    def sorted(self) -> LineageDocument:
        """Return a copy with every list ordered so serialization is deterministic."""
        self = self.model_copy(deep=True)
        for dataset in self.datasets:
            dataset.aliases = sorted(set(dataset.aliases))
            dataset.columns = sorted(set(dataset.columns))
        for job in self.jobs:
            job.inputs = sorted(set(job.inputs))
            job.outputs = sorted(set(job.outputs))
        for schedule in self.schedules.values():
            schedule.tags = sorted(set(schedule.tags))
            schedule.declared_upstream = sorted(set(schedule.declared_upstream))
            schedule.declared_downstream = sorted(set(schedule.declared_downstream))
        for dependencies in self.job_dependencies.values():
            for dependency in dependencies:
                dependency.sources = sorted(set(dependency.sources))
                dependency.via_datasets = sorted(set(dependency.via_datasets))
        for edge in self.column_edges:
            edge.sources = sorted(edge.sources, key=lambda r: r.model_dump_json())
            edge.indirect_sources = sorted(edge.indirect_sources, key=lambda r: r.model_dump_json())
        for product in self.products:
            product.owners = sorted(set(product.owners))
            product.databases = sorted(product.databases, key=lambda d: d.name)
            for dependency in product.declared_dependencies:
                dependency.tables = sorted(set(dependency.tables))
            product.declared_dependencies.sort(key=lambda d: d.model_dump_json())
        for unresolved in self.unresolved:
            unresolved.symbols = sorted(set(unresolved.symbols))
            unresolved.assumptions = dict(sorted(unresolved.assumptions.items()))
        return self.model_copy(
            update={
                "products": sorted(self.products, key=lambda p: p.code),
                "datasets": sorted(self.datasets, key=lambda d: d.id),
                "jobs": sorted(self.jobs, key=lambda j: j.id),
                "schedules": dict(sorted(self.schedules.items())),
                "task_jobs": dict(sorted(self.task_jobs.items())),
                "job_dependencies": {
                    k: sorted(v, key=lambda d: d.job_id)
                    for k, v in sorted(self.job_dependencies.items())
                },
                "column_edges": sorted(
                    self.column_edges,
                    key=lambda e: (
                        e.job_id,
                        e.target.dataset_id,
                        e.target.name,
                        e.model_dump_json(),
                    ),
                ),
                "table_edges": sorted(
                    self.table_edges,
                    key=lambda e: (e.job_id, e.source, e.target, e.model_dump_json()),
                ),
                "unresolved": sorted(
                    self.unresolved,
                    key=lambda u: (u.source_file or "", u.line or 0, u.kind, u.reason),
                ),
            }
        )

    def summary(self) -> dict[str, dict[str, int]]:
        """Count column edges by confidence and unresolved items by kind (section 13).

        Returns:
            dict[str, dict[str, int]]: ``{"column_edges_by_confidence": {...},
            "unresolved_by_kind": {...}}`` so scan coverage is visible at a glance.
        """
        by_conf: dict[str, int] = {}
        for e in self.column_edges:
            by_conf[e.provenance.confidence] = by_conf.get(e.provenance.confidence, 0) + 1
        by_kind: dict[str, int] = {}
        for u in self.unresolved:
            by_kind[u.kind] = by_kind.get(u.kind, 0) + 1
        return {"column_edges_by_confidence": by_conf, "unresolved_by_kind": by_kind}
