"""Observed product dependencies and orchestration drift."""


def product_dependencies(graph, registry=None):
    doc = graph.document
    owners = {d.id: d.product for d in doc.datasets}
    observed = {}
    for edge in doc.table_edges:
        consumer, producer = owners.get(edge.target), owners.get(edge.source)
        if consumer and producer and consumer != producer:
            observed.setdefault((consumer, producer), set()).add(edge.source)
    declared = {
        (product.code, dependency.code)
        for product in doc.products
        for dependency in product.declared_dependencies
    }
    return [
        {
            "from_product": a,
            "to_product": b,
            "datasets": sorted(observed.get((a, b), [])),
            "status": "confirmed"
            if (a, b) in observed and (a, b) in declared
            else "undeclared"
            if (a, b) in observed
            else "declared_but_unobserved",
        }
        for a, b in sorted(set(observed) | declared)
    ]


def orchestration_drift(graph):
    linked = {
        j.id
        for j in graph.document.jobs
        if j.schedule_id and graph.document.schedules[j.schedule_id].orchestrator != "cron_comment"
    }
    return [
        {
            "job_id": job,
            "upstream": dep.job_id,
            "status": "missing_task_dependency" if dep.sources == ["data"] else "ordering_only",
        }
        for job, deps in graph.document.job_dependencies.items()
        for dep in deps
        if job in linked and dep.job_id in linked and len(dep.sources) == 1
    ]
