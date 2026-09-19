"""Merge parser results and derive data and orchestrator dependencies (spec section 9).

Implements the graph builder: merges every worker's
:class:`~etl_parser.models.WorkerResult`, applies dataset identity rules (unifying aliases
via :class:`~etl_parser.identity.DatasetRegistry`), attaches product/layer ownership from
the :class:`~etl_parser.registry.ProductRegistry`, derives job dependencies from both data
flow and orchestrator-declared task order, and detects task-graph cycles. The result is a
:class:`LineageGraph` wrapping a ``networkx.MultiDiGraph`` of datasets, columns, and jobs,
consumed by the exporters, :mod:`etl_parser.graph.impact`, and
:mod:`etl_parser.graph.products`.
"""

from collections import defaultdict

import networkx as nx

from etl_parser.identity import DatasetRegistry, split_dataset_id
from etl_parser.models import (
    JobDependency,
    LineageDocument,
    Provenance,
    TableEdge,
    Unresolved,
    WorkerResult,
)
from etl_parser.registry import ProductRegistry

JOB_IO_PARSER = "job_io"
"""Provenance parser name for input-by-output fallback table edges (finding 12).

These edges record that a job declared a read and a write, not that a parser traced data
from one to the other, so they carry ``confidence="partial"`` and are excluded from
column-level exports such as the OpenLineage column lineage facet.
"""


class LineageGraph:
    """A lineage document plus the ``networkx.MultiDiGraph`` built from it.

    Nodes are dataset ids and ``"{dataset_id}#{column}"`` column ids; edges are ``table``
    (dataset to dataset, tagged with the producing ``job``) and ``column`` (column to
    column, also tagged with ``job``). Used by :mod:`etl_parser.graph.impact` and
    :mod:`etl_parser.graph.products` for graph traversal, and by the CLI/exporters via
    :meth:`to_document`.

    Attributes:
        document: The sorted :class:`~etl_parser.models.LineageDocument` this graph was
            built from.
        graph: The ``networkx.MultiDiGraph`` of datasets, columns, and their edges.
    """

    def __init__(self, document: LineageDocument):
        """Build the traversal graph from an already-assembled lineage document.

        Args:
            document: The lineage document to wrap. Stored sorted (see
                :meth:`~etl_parser.models.LineageDocument.sorted`); the input is not
                mutated.
        """
        self.document = document.sorted()
        self.graph = nx.MultiDiGraph()
        for dataset in document.datasets:
            self.graph.add_node(dataset.id, kind="dataset", product=dataset.product)
        for edge in document.table_edges:
            self.graph.add_edge(edge.source, edge.target, kind="table", job=edge.job_id)
        for edge in document.column_edges:
            target = f"{edge.target.dataset_id}#{edge.target.name}"
            self.graph.add_node(target, kind="column", dataset=edge.target.dataset_id)
            for ref in edge.sources + edge.indirect_sources:
                source = f"{ref.dataset_id}#{ref.name}"
                self.graph.add_node(source, kind="column", dataset=ref.dataset_id)
                self.graph.add_edge(source, target, kind="column", job=edge.job_id)

    def to_document(self):
        """Return a freshly sorted copy of the underlying lineage document.

        Returns:
            LineageDocument: A sorted copy, suitable for deterministic serialization.
        """
        return self.document.sorted()

    def job_dependencies(self):
        """Return each job's resolved upstream dependencies.

        Returns:
            dict[str, list[JobDependency]]: Job id to its list of upstream dependencies,
            as stored on the underlying document.
        """
        return self.document.job_dependencies


def build_graph(
    results: list[WorkerResult],
    registry: ProductRegistry | None = None,
    scan_commit: str | None = None,
) -> LineageGraph:
    """Merge worker results into one identity-resolved, dependency-derived lineage graph.

    Steps (spec section 9): merge every result; resolve dataset aliases through a
    :class:`~etl_parser.identity.DatasetRegistry`, flagging conflicting aliases as
    unresolved; stamp ``scan_commit`` onto edges that lack one; merge duplicate jobs;
    attach product/layer ownership and product-declared schedules from ``registry``; derive
    ``data`` job dependencies (an output of job A read by job B, where A is not an in-place
    writer of that dataset) and ``dag`` job dependencies (from declared task order,
    including cross-DAG references); detect task-graph cycles; and deduplicate edges.

    Args:
        results: The :class:`~etl_parser.models.WorkerResult` from every parser invocation,
            plus any pre-seeded unresolved items (e.g. from source indexing).
        registry: Product registry to attach ownership and schedules from. If omitted, no
            product/layer attachment is performed and the document's ``products`` is empty.
        scan_commit: Commit to stamp onto edges and the resulting document.

    Returns:
        LineageGraph: The built graph, wrapping a fully assembled, deduplicated
        :class:`~etl_parser.models.LineageDocument`.
    """
    combined = WorkerResult()
    for result in results:
        combined.extend(result.model_copy(deep=True))
    datasets = DatasetRegistry()
    for dataset in combined.datasets:
        datasets.add(dataset)
    aliases = defaultdict(set)
    for dataset in combined.datasets:
        for alias in dataset.aliases:
            if "://" in alias and alias != dataset.id:
                aliases[alias].add(dataset.id)
    for alias, owners in sorted(aliases.items()):
        if len(owners) == 1:
            datasets.merge_alias(alias, next(iter(owners)))
        else:
            combined.unresolved.append(
                Unresolved(
                    kind="unsupported_syntax",
                    reason=f"Conflicting dataset alias {alias}: {sorted(owners)}",
                    remediation="Declare one canonical dataset for this alias.",
                )
            )
    for edge in combined.table_edges:
        if scan_commit and edge.provenance.scan_commit is None:
            edge.provenance.scan_commit = scan_commit
        edge.source = datasets.resolve_id(edge.source)
        edge.target = datasets.resolve_id(edge.target)
        datasets.get_or_create(edge.source)
        datasets.get_or_create(edge.target)
    for edge in combined.column_edges:
        if scan_commit and edge.provenance.scan_commit is None:
            edge.provenance.scan_commit = scan_commit
        for ref in [edge.target, *edge.sources, *edge.indirect_sources]:
            ref.dataset_id = datasets.resolve_id(ref.dataset_id)
            ds = datasets.get_or_create(ref.dataset_id)
            datasets.add(ds.model_copy(update={"columns": [ref.name]}))
    jobs = {}
    for job in combined.jobs:
        job.inputs = sorted({datasets.resolve_id(i) for i in job.inputs})
        job.outputs = sorted({datasets.resolve_id(i) for i in job.outputs})
        if job.id in jobs:
            old = jobs[job.id]
            old.inputs = sorted(set(old.inputs) | set(job.inputs))
            old.outputs = sorted(set(old.outputs) | set(job.outputs))
        else:
            jobs[job.id] = job.model_copy(deep=True)
        for ident in job.inputs + job.outputs:
            datasets.get_or_create(ident)
    refs = datasets.all()
    schedules = dict(combined.schedules)
    if registry:
        registry.resolve_dependencies()
        schedules.update(registry.schedules())
        for ds in refs:
            scheme, namespace, _ = split_dataset_id(ds.id)
            # The engine is part of ownership: a product's Athena warehouse does not own a
            # same-named Postgres database (finding 30).
            product = registry.product_for_database(namespace, scheme)
            if product:
                ds.product = product.code
                ds.layer = registry.layer_for_database(namespace, scheme)
                continue
            declared = registry.product_for_database(namespace)
            if declared:
                combined.unresolved.append(
                    Unresolved(
                        kind="analysis_note",
                        reason=(
                            f"Database {namespace!r} is declared by product "
                            f"{declared.code!r} for another engine, so {ds.id} is left "
                            "unattributed"
                        ),
                        remediation=(
                            "Declare this engine in the product's databases, or correct "
                            "the connection the job uses."
                        ),
                    )
                )
        products = {d.id: d.product for d in refs}
        for job in jobs.values():
            candidates = {products.get(d) for d in job.outputs} - {None}
            if len(candidates) == 1:
                job.product = candidates.pop()
            if not job.product:
                for product in registry.products:
                    directory = (product.source_file or "").rsplit("/", 1)[0]
                    if directory and job.source_file.startswith(directory + "/"):
                        job.product = product.code
            if job.product:
                ident = f"product.{job.product}.{job.name}"
                if ident in schedules:
                    job.schedule_id = ident
    for schedule_id, job_id in combined.task_jobs.items():
        if job_id in jobs:
            jobs[job_id].schedule_id = schedule_id
    writers = defaultdict(set)
    for job in jobs.values():
        for dataset in job.outputs:
            writers[dataset].add(job.id)
    dependencies: dict[str, dict[str, JobDependency]] = {job: {} for job in jobs}

    def add(job, upstream, evidence, dataset=None):
        """Record that ``job`` depends on ``upstream``, merging evidence if already known.

        ``in_place_writer`` is computed per linking dataset (review finding 29): it is true
        only when ``upstream`` both reads and writes one of the datasets that actually link
        the two jobs, not merely when it reads and writes something.

        Args:
            job: Id of the dependent job.
            upstream: Id of the job it depends on.
            evidence: ``"data"`` or ``"dag"``, the dependency source being added.
            dataset: Dataset id that links the two jobs, for data evidence.
        """
        if job == upstream or job not in jobs or upstream not in jobs:
            return
        dep = dependencies[job].setdefault(upstream, JobDependency(job_id=upstream))
        dep.sources = sorted(set(dep.sources) | {evidence})
        if dataset:
            dep.via_datasets = sorted(set(dep.via_datasets) | {dataset})
            parent = jobs[upstream]
            if dataset in parent.inputs and dataset in parent.outputs:
                dep.in_place_writer = True

    # An in-place writer is only excluded while some other job also writes the dataset, in
    # which case that other job is the producer and ordering is unknowable (finding 3). A
    # sole writer is always the producer, flagged so consumers can see it rewrites in place.
    for job in jobs.values():
        for dataset in job.inputs:
            for upstream in writers[dataset]:
                in_place = dataset in jobs[upstream].inputs
                if in_place and writers[dataset] - {upstream}:
                    continue
                add(job.id, upstream, "data", dataset)
    # Task-only nodes (e.g. EmptyOperator) still carry ordering between linked jobs.
    task_graph = nx.DiGraph()

    def external_refs(reference, downstream, schedule):
        """Resolve an ``external-dag://`` cross-DAG reference to matching schedule ids.

        Plain (non-``external-dag://``) references pass through unchanged. Ambiguous or
        unmatched external references are recorded as an ``external_job`` unresolved item
        and resolve to no ids.

        Args:
            reference: A schedule id or an ``external-dag://<dag_id>/<task_id>`` reference.
            downstream: True when resolving a declared downstream edge, which targets the
                DAG root when no task id is given; False targets the DAG's tasks.
            schedule: The schedule that declared the reference, used for diagnostics.

        Returns:
            list[str]: Matching schedule ids, possibly empty.
        """
        if not reference.startswith("external-dag://"):
            return [reference]
        dag_id, _, task_id = reference.removeprefix("external-dag://").partition("/")
        roots = [s for s in combined.schedules.values() if s.dag_id == dag_id and s.task_id is None]
        matches = [
            s.id
            for s in combined.schedules.values()
            if s.dag_id == dag_id
            and (
                s.task_id == task_id
                if task_id
                else s.task_id is None
                if downstream
                else bool(s.task_id)
            )
        ]
        if len(roots) != 1 or not matches:
            combined.unresolved.append(
                Unresolved(
                    kind="external_job",
                    source_file=schedule.source_file,
                    line=schedule.line,
                    reason=f"External DAG/task cannot be resolved unambiguously: {reference}",
                )
            )
            return []
        return matches

    for schedule in combined.schedules.values():
        schedule.declared_upstream = sorted(
            {
                item
                for ref in schedule.declared_upstream
                for item in external_refs(ref, False, schedule)
            }
        )
        schedule.declared_downstream = sorted(
            {
                item
                for ref in schedule.declared_downstream
                for item in external_refs(ref, True, schedule)
            }
        )
    for schedule in combined.schedules.values():
        task_graph.add_node(schedule.id)
        for upstream in schedule.declared_upstream:
            if upstream in combined.schedules:
                task_graph.add_edge(upstream, schedule.id)
        for target in schedule.declared_downstream:
            if target in combined.schedules:
                task_graph.add_edge(schedule.id, target)
        if schedule.dag_id and schedule.task_id:
            roots = [
                s
                for s in combined.schedules.values()
                if s.dag_id == schedule.dag_id
                and s.source_file == schedule.source_file
                and s.task_id is None
            ]
            for root in roots:
                task_graph.add_edge(root.id, schedule.id)
    if not nx.is_directed_acyclic_graph(task_graph):
        combined.unresolved.append(
            Unresolved(kind="unsupported_syntax", reason="Orchestrator task graph contains a cycle")
        )
    for task, job in combined.task_jobs.items():
        if not job or task not in task_graph:
            continue
        for ancestor in nx.ancestors(task_graph, task):
            parent = combined.task_jobs.get(ancestor)
            if parent:
                add(job, parent, "dag")

    # Declared reads with no parsed edge would otherwise be invisible to impact analysis
    # (finding 12), so every unlinked input/output pair gets a partial-confidence fallback.
    # These run after dependency derivation, which reads inputs/outputs and never edges.
    linked = {(edge.source, edge.target) for edge in combined.table_edges}
    for job in sorted(jobs.values(), key=lambda j: j.id):
        for source in sorted(set(job.inputs)):
            for target in sorted(set(job.outputs)):
                if source == target or (source, target) in linked:
                    continue
                linked.add((source, target))
                combined.table_edges.append(
                    TableEdge(
                        source=source,
                        target=target,
                        job_id=job.id,
                        source_file=job.source_file,
                        provenance=Provenance(
                            parser=JOB_IO_PARSER,
                            confidence="partial",
                            scan_commit=scan_commit,
                        ),
                    )
                )

    # Stable deduplication includes full edge metadata, preserving distinct transformations.
    def unique(items):
        """Deduplicate model instances by their full JSON representation, preserving order.

        Args:
            items: Pydantic model instances (edges or unresolved items).

        Returns:
            list: The first occurrence of each distinct instance, in input order.
        """
        return list({item.model_dump_json(): item for item in items}.values())

    document = LineageDocument(
        scan_commit=scan_commit,
        products=registry.products if registry else [],
        datasets=refs,
        jobs=list(jobs.values()),
        schedules=schedules,
        task_jobs=combined.task_jobs,
        job_dependencies={job: list(deps.values()) for job, deps in dependencies.items()},
        table_edges=unique(combined.table_edges),
        column_edges=unique(combined.column_edges),
        unresolved=unique(combined.unresolved),
    )
    return LineageGraph(document)
