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
]
Orchestrator = Literal["airflow", "product_yaml", "cron_comment"]
DependencySource = Literal["data", "dag"]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DatasetRef(_Model):
    id: str
    namespace: str
    name: str
    kind: DatasetKind = "table"
    aliases: list[str] = Field(default_factory=list)
    physical_location: str | None = None
    product: str | None = None
    layer: str | None = None
    columns: list[str] = Field(default_factory=list)


class ColumnRef(_Model):
    dataset_id: str
    name: str
    datatype: str | None = None


class Transformation(_Model):
    expression: str | None = None
    kind: TransformationKind = "unknown"
    source_file: str | None = None
    line_start: int | None = None
    line_end: int | None = None


class Provenance(_Model):
    parser: Parser
    confidence: Confidence = "exact"
    dialect: str | None = None
    scan_commit: str | None = None


class ColumnEdge(_Model):
    target: ColumnRef
    sources: list[ColumnRef] = Field(default_factory=list)
    indirect_sources: list[ColumnRef] = Field(default_factory=list)
    transformation: Transformation = Field(default_factory=Transformation)
    provenance: Provenance
    job_id: str


class TableEdge(_Model):
    source: str
    target: str
    provenance: Provenance
    job_id: str
    source_file: str | None = None
    line: int | None = None


class Schedule(_Model):
    id: str
    orchestrator: Orchestrator
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
    id: str
    name: str
    source_file: str
    language: Language = "unknown"
    engine: Engine = "unknown"
    dialect: str | None = None
    schedule_id: str | None = None
    product: str | None = None
    owner: str | None = None
    description: str | None = None
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)


class JobDependency(_Model):
    job_id: str
    sources: list[DependencySource] = Field(default_factory=list)
    via_datasets: list[str] = Field(default_factory=list)
    in_place_writer: bool = False


class Unresolved(_Model):
    kind: UnresolvedKind
    source_file: str | None = None
    line: int | None = None
    reason: str
    partial_text: str | None = None
    job_id: str | None = None


class ProductDatabase(_Model):
    name: str
    type: str | None = None
    layer: str | None = None
    description: str | None = None


class DeclaredDependency(_Model):
    product: str | None = None
    code: str
    tables: list[str] = Field(default_factory=list)


class Product(_Model):
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
    datasets: list[DatasetRef] = Field(default_factory=list)
    jobs: list[Job] = Field(default_factory=list)
    schedules: dict[str, Schedule] = Field(default_factory=dict)
    task_jobs: dict[str, str | None] = Field(default_factory=dict)
    column_edges: list[ColumnEdge] = Field(default_factory=list)
    table_edges: list[TableEdge] = Field(default_factory=list)
    unresolved: list[Unresolved] = Field(default_factory=list)

    def extend(self, other: WorkerResult) -> WorkerResult:
        self.datasets.extend(other.datasets)
        self.jobs.extend(other.jobs)
        self.schedules.update(other.schedules)
        self.task_jobs.update(other.task_jobs)
        self.column_edges.extend(other.column_edges)
        self.table_edges.extend(other.table_edges)
        self.unresolved.extend(other.unresolved)
        return self


class LineageDocument(_Model):
    version: str = "1"
    generated_at: datetime | None = None
    scan_commit: str | None = None
    products: list[Product] = Field(default_factory=list)
    datasets: list[DatasetRef] = Field(default_factory=list)
    jobs: list[Job] = Field(default_factory=list)
    schedules: dict[str, Schedule] = Field(default_factory=dict)
    job_dependencies: dict[str, list[JobDependency]] = Field(default_factory=dict)
    column_edges: list[ColumnEdge] = Field(default_factory=list)
    table_edges: list[TableEdge] = Field(default_factory=list)
    unresolved: list[Unresolved] = Field(default_factory=list)

    def sorted(self) -> LineageDocument:
        """Return a copy with every list ordered so serialization is deterministic."""
        return self.model_copy(
            update={
                "products": sorted(self.products, key=lambda p: p.code),
                "datasets": sorted(self.datasets, key=lambda d: d.id),
                "jobs": sorted(self.jobs, key=lambda j: j.id),
                "schedules": dict(sorted(self.schedules.items())),
                "job_dependencies": {
                    k: sorted(v, key=lambda d: d.job_id)
                    for k, v in sorted(self.job_dependencies.items())
                },
                "column_edges": sorted(
                    self.column_edges,
                    key=lambda e: (e.job_id, e.target.dataset_id, e.target.name),
                ),
                "table_edges": sorted(
                    self.table_edges, key=lambda e: (e.job_id, e.source, e.target)
                ),
                "unresolved": sorted(
                    self.unresolved,
                    key=lambda u: (u.source_file or "", u.line or 0, u.kind, u.reason),
                ),
            }
        )

    def summary(self) -> dict[str, dict[str, int]]:
        by_conf: dict[str, int] = {}
        for e in self.column_edges:
            by_conf[e.provenance.confidence] = by_conf.get(e.provenance.confidence, 0) + 1
        by_kind: dict[str, int] = {}
        for u in self.unresolved:
            by_kind[u.kind] = by_kind.get(u.kind, 0) + 1
        return {"column_edges_by_confidence": by_conf, "unresolved_by_kind": by_kind}
