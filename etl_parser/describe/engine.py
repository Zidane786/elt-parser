"""Optional topological description enrichment, preserving existing descriptions."""

import copy
import json

import networkx as nx

from etl_parser.describe.prompt import build_prompt


class DescriptionEngine:
    def __init__(self, client):
        self.client = client
        self.warnings = []

    def run(self, doc, catalog):
        self.warnings = []
        catalog = copy.deepcopy(catalog)
        columns = {}
        by_table = {}
        ids = {(d.namespace.split("://", 1)[-1], d.name): d.id for d in doc.datasets}
        for database in catalog.get("databases", []):
            for table in database.get("tables", []):
                ident = table.get("dataset_id") or ids.get(
                    (database["db_name"], table["table_name"])
                )
                if ident:
                    by_table[ident] = table
                    for column in table.get("schema", []):
                        columns[(ident, column["field_name"])] = column
        graph = nx.DiGraph()
        edges_by_target = {}
        for edge in doc.column_edges:
            target = (edge.target.dataset_id, edge.target.name)
            graph.add_node(target)
            edges_by_target.setdefault(target, []).append(edge)
            for source in edge.sources:
                origin = (source.dataset_id, source.name)
                if origin != target:
                    graph.add_edge(origin, target)
        # Condensation keeps acyclic portions useful even when in-place jobs form cycles.
        components = nx.condensation(graph)
        order = [
            node
            for component in nx.lexicographical_topological_sort(components)
            for node in sorted(components.nodes[component]["members"])
        ]
        for target in order:
            column = columns.get(target)
            edges = edges_by_target.get(target, [])
            if column is None or column.get("description") or not edges:
                continue
            sources = {(s.dataset_id, s.name) for edge in edges for s in edge.sources}
            known = {
                f"{d}#{c}": columns.get((d, c), {}).get("description", "")
                for d, c in sorted(sources)
            }
            if len(sources) == 1 and all(
                e.transformation.kind == "identity" and e.provenance.confidence == "exact"
                for e in edges
            ):
                description = next(iter(known.values()))
                if description:
                    column.update(description=description, description_source="inherited")
                    continue
            if any(e.provenance.confidence != "exact" for e in edges):
                self.warnings.append(f"Skipped incomplete lineage: {target}")
                continue
            prompt = build_prompt(
                edges[0].target, edges, column, known, by_table[target[0]].get("description")
            )
            try:
                response = json.loads(self.client.complete(prompt.system, prompt.user))
                if not isinstance(response, dict) or not isinstance(
                    response.get("description"), str
                ):
                    raise ValueError("Response must contain a description string")
                column.update(description=response["description"], description_source="ai")
            except Exception as exc:
                self.warnings.append(f"Skipped {target}: {exc}")
        return catalog
