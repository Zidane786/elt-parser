"""Optional topological description enrichment, preserving existing descriptions."""

import asyncio
import copy
import json
from typing import TYPE_CHECKING
from uuid import uuid4

import networkx as nx

from etl_parser.describe.prompt import build_prompt
from etl_parser.observability import current_observer, digest, observed

if TYPE_CHECKING:
    from agent_sdk import LLMRunnerProtocol


class DescriptionEngine:
    def __init__(self, runner: "LLMRunnerProtocol", *, model: str, max_tokens: int = 1024):
        if not model.strip():
            raise ValueError("An explicit SDK model ID or registered model slug is required")
        if max_tokens < 1:
            raise ValueError("max_tokens must be positive")
        self.runner = runner
        self.model = model
        self.max_tokens = max_tokens
        self.warnings = []

    def run(self, doc, catalog, *, log_dir=None, log_level="INFO", observer=None):
        """Synchronous entry point. Async applications should await ``arun``."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.arun(doc, catalog, log_dir=log_dir, log_level=log_level, observer=observer)
            )
        raise RuntimeError("An event loop is running; use await engine.arun(doc, catalog)")

    @observed("descriptions")
    async def arun(self, doc, catalog, *, log_dir=None, log_level="INFO", observer=None):
        from agent_sdk.types import Message

        observer = current_observer()
        observer.configure(
            model=self.model, max_output_tokens=self.max_tokens, ai_lineage="off", descriptions=True
        )
        observer.event(
            "description.started",
            actor="description_policy",
            model=self.model,
            max_output_tokens=self.max_tokens,
            ai_lineage="off",
        )
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
                reason = (
                    "existing_description" if column and column.get("description") else "no_target"
                )
                observer.count(f"descriptions.skipped.{reason}")
                observer.event(
                    "description.skipped",
                    level="DEBUG",
                    actor="description_policy",
                    target=target,
                    reason=reason,
                )
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
                    observer.count("descriptions.inherited")
                    observer.event("description.inherited", level="DEBUG", target=target)
                    continue
            if any(e.provenance.confidence != "exact" for e in edges):
                self.warnings.append(f"Skipped incomplete lineage: {target}")
                observer.count("descriptions.skipped.incomplete_lineage")
                observer.partial()
                observer.event(
                    "description.skipped",
                    level="WARNING",
                    actor="description_policy",
                    target=target,
                    reason="incomplete_lineage",
                )
                continue
            prompt = build_prompt(
                edges[0].target, edges, column, known, by_table[target[0]].get("description")
            )
            attempted = completed = False
            request_id = uuid4().hex
            try:
                if observer.sink_failed:
                    raise RuntimeError("Persistent audit unavailable; skipping further AI calls")
                observer.count("ai.calls.attempted")
                attempted = True
                observer.event(
                    "ai.request.started",
                    actor="description_policy",
                    target=target,
                    model=self.model,
                    reason="missing_description_exact_lineage",
                    prompt_digest=digest(prompt.system + prompt.user),
                    prompt_chars=len(prompt.system) + len(prompt.user),
                    tools_enabled=False,
                    request_id=request_id,
                )
                with observer.span("ai.complete", model=self.model, request_id=request_id):
                    completion = await self.runner.complete(
                        messages=[Message(role="user", content=prompt.user)],
                        system=prompt.system,
                        tools=[],
                        model=self.model,
                        max_tokens=self.max_tokens,
                        temperature=0.0,
                    )
                observer.count("ai.calls.completed")
                completed = True
                usage = completion.usage.model_dump(exclude={"raw"}) if completion.usage else {}
                observer.event(
                    "ai.response.received",
                    actor="agent_sdk",
                    target=target,
                    stop_reason=completion.stop_reason,
                    usage=usage,
                    response_digest=digest(json.dumps(completion.content, sort_keys=True)),
                    request_id=request_id,
                )
                for key in (
                    "input_tokens",
                    "output_tokens",
                    "reasoning_tokens",
                    "cache_read_tokens",
                    "cache_write_tokens",
                ):
                    value = usage.get(key)
                    if isinstance(value, int):
                        observer.count(f"ai.usage.{key}", value)
                if not any(
                    usage.get(key, 0)
                    for key in (
                        "input_tokens",
                        "output_tokens",
                        "cache_read_tokens",
                        "cache_write_tokens",
                    )
                ):
                    observer.count("ai.usage.zero_or_unreported_responses")
                if completion.stop_reason not in {None, "end_turn", "stop", "stop_sequence"}:
                    raise ValueError(f"Incomplete or blocked response: {completion.stop_reason}")
                if any(b.get("type") == "tool_use" for b in completion.content):
                    raise ValueError("Unexpected tool request; descriptions cannot execute tools")
                response = json.loads(
                    "".join(b["text"] for b in completion.content if b.get("type") == "text")
                )
                if (
                    not isinstance(response, dict)
                    or not isinstance(response.get("description"), str)
                    or not response["description"].strip()
                ):
                    raise ValueError("Response must contain a non-empty description string")
                column.update(description=response["description"], description_source="ai")
                observer.count("descriptions.generated")
                observer.event(
                    "description.accepted",
                    actor="response_validator",
                    target=target,
                    description_digest=digest(response["description"]),
                    reason="nonempty_valid_response",
                    request_id=request_id,
                )
            except Exception as exc:
                self.warnings.append(f"Skipped {target}: {type(exc).__name__}")
                if attempted:
                    observer.count("ai.responses.rejected" if completed else "ai.calls.failed")
                observer.count("descriptions.failed")
                observer.partial()
                observer.event(
                    "description.rejected",
                    level="WARNING",
                    actor="response_validator",
                    target=target,
                    error_type=type(exc).__name__,
                    request_id=request_id,
                )
        observer.gauge("descriptions.warnings", len(self.warnings))
        return catalog
