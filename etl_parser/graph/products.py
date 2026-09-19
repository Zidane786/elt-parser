"""Observed product dependencies and orchestration drift (spec sections 8.4, 9).

Implements the product dependency graph and drift checks: :func:`product_dependencies`
compares data-observed cross-product table edges against each product's declared
dependencies, and :func:`orchestration_drift` compares data-derived job order against
orchestrator-declared task order.
"""


def product_dependencies(graph, registry=None):
    """Compare observed cross-product table edges against declared dependencies.

    Args:
        graph: The lineage graph to inspect.
        registry: Unused; declared dependencies are read from ``graph.document.products``.

    Returns:
        list[dict]: One entry per ``(consumer, producer)`` product pair that is either
        observed (a table edge crosses products) or declared, sorted by that pair. Each
        entry has ``from_product``, ``to_product``, the ``datasets`` observed to cross
        between them, and a ``status`` of ``"confirmed"`` (observed and declared),
        ``"undeclared"`` (observed only), or ``"declared_but_unobserved"`` (declared only).
    """
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
    """Find job dependency pairs missing a declared task order or missing a data edge.

    Only considers jobs linked to a non-comment-derived schedule (Airflow or
    ``product.yaml``), and only dependencies with exactly one evidence source, since a
    dependency with both ``data`` and ``dag`` evidence is not drift.

    Args:
        graph: The lineage graph to inspect.

    Returns:
        list[dict]: One entry per single-evidence dependency between two scheduled jobs,
        with ``job_id``, ``upstream``, and a ``status`` of ``"missing_task_dependency"``
        (data flow observed but the orchestrator does not declare the order) or
        ``"ordering_only"`` (the orchestrator declares the order but no data flow was
        observed between them).
    """
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
