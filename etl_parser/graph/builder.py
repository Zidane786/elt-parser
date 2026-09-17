"""Merge parser results and derive data and orchestrator dependencies."""

from collections import defaultdict

import networkx as nx

from etl_parser.identity import DatasetRegistry, split_dataset_id
from etl_parser.models import JobDependency, LineageDocument, Unresolved, WorkerResult
from etl_parser.registry import ProductRegistry


class LineageGraph:
    def __init__(self, document: LineageDocument):
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
        return self.document.sorted()

    def job_dependencies(self):
        return self.document.job_dependencies


def build_graph(
    results: list[WorkerResult],
    registry: ProductRegistry | None = None,
    scan_commit: str | None = None,
) -> LineageGraph:
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
            _, namespace, _ = split_dataset_id(ds.id)
            product = registry.product_for_database(namespace)
            if product:
                ds.product, ds.layer = product.code, registry.layer_for_database(namespace)
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
        if job == upstream or job not in jobs or upstream not in jobs:
            return
        dep = dependencies[job].setdefault(upstream, JobDependency(job_id=upstream))
        dep.sources = sorted(set(dep.sources) | {evidence})
        if dataset:
            dep.via_datasets = sorted(set(dep.via_datasets) | {dataset})
        parent = jobs[upstream]
        dep.in_place_writer = bool(set(parent.inputs) & set(parent.outputs))

    for job in jobs.values():
        for dataset in job.inputs:
            for upstream in writers[dataset]:
                if dataset not in jobs[upstream].inputs:
                    add(job.id, upstream, "data", dataset)
    # Task-only nodes (e.g. EmptyOperator) still carry ordering between linked jobs.
    task_graph = nx.DiGraph()

    def external_refs(reference, downstream, schedule):
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

    # Stable deduplication includes full edge metadata, preserving distinct transformations.
    def unique(items):
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
