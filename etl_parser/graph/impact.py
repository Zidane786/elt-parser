"""Bounded graph traversal, cycle safe, with explicit hop distance."""

import networkx as nx

from etl_parser.graph.builder import LineageGraph


def _walk(graph: LineageGraph, node_id: str, max_depth: int | None, reverse: bool):
    if max_depth is not None and max_depth < 0:
        raise ValueError("max_depth must be nonnegative")
    net = graph.graph.reverse(copy=False) if reverse else graph.graph
    if node_id not in net:
        raise ValueError(f"Unknown dataset/column: {node_id}")
    distances = nx.single_source_shortest_path_length(net, node_id, cutoff=max_depth)
    by_hop = []
    doc = graph.document
    products = {d.id: d.product for d in doc.datasets}
    for distance in sorted(set(distances.values()) - {0}):
        nodes = sorted(n for n, depth in distances.items() if depth == distance)
        columns = [n for n in nodes if net.nodes[n].get("kind") == "column"]
        datasets = sorted({net.nodes[n].get("dataset", n) for n in nodes})
        jobs = set()
        for n in nodes:
            for parent, _, data in net.in_edges(n, data=True):
                if parent in distances and distances[parent] == distance - 1:
                    jobs.add(data["job"])
        by_hop.append(
            {
                "hop": distance,
                "datasets": datasets,
                "columns": columns,
                "jobs": sorted(jobs),
                "products": sorted({products.get(d) for d in datasets} - {None}),
            }
        )
    crossed = []
    for edge in doc.table_edges:
        a, b = products.get(edge.source), products.get(edge.target)
        if a and b and a != b and edge.source in distances and edge.target in distances:
            crossed.append(
                {"source": edge.source, "target": edge.target, "from_product": a, "to_product": b}
            )
    return {
        "node": node_id,
        "direction": "upstream" if reverse else "downstream",
        "by_hop": by_hop,
        "cross_product_edges": crossed,
    }


def downstream(graph, node_id, max_depth=None):
    return _walk(graph, node_id, max_depth, False)


def upstream(graph, node_id, max_depth=None):
    return _walk(graph, node_id, max_depth, True)
