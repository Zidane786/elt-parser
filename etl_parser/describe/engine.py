"""Optional topological description enrichment, preserving existing descriptions.

Implements the ``DescriptionEngine`` from spec section 11: orders tables so upstream
descriptions exist before downstream prompts are built, skips columns that already have a
description or only inherit one through an identity edge, and writes AI-generated
descriptions with ``description_source: ai``. One request covers a whole table — every
column of it that still needs a description, plus the table's own description — so the
table and its columns are described consistently and the call count follows the number of
tables, not the number of columns. No LLM is used to infer lineage; deterministic parts
(ordering, batching, skip policy, evidence assembly) live here, the LLM call is delegated
to a caller/CLI-selected runner (see ``etl_parser.describe.client``).
"""

import asyncio
import copy
import json
from typing import TYPE_CHECKING
from uuid import uuid4

import networkx as nx
from pydantic import BaseModel, ConfigDict, Field

from etl_parser.describe.prompt import build_table_prompt
from etl_parser.observability import current_observer, digest, observed

if TYPE_CHECKING:
    from agent_sdk import LLMRunnerProtocol

REGENERATABLE_SOURCES = frozenset({"ai", "code"})
"""Description sources this engine may replace when ``override_existing`` is set.

Anything else, ``human`` and ``verified`` above all, is a person's text and is kept.
"""


class _Response(BaseModel):
    """Base for describe response models; rejects any field not declared here."""

    model_config = ConfigDict(extra="forbid")


class ColumnDescription(_Response):
    """One described column in a batched describe response.

    Attributes:
        name: Field name of the column being described; must be one that was requested.
        description: The proposed description text.
        confidence: The model's own 0-1 self-assessment of this description.
        rationale: Short model-supplied reason behind ``confidence``.
    """

    name: str
    description: str = Field(min_length=1, max_length=10000)
    confidence: float = Field(ge=0, le=1)
    rationale: str = Field(max_length=500)


class TableDescription(_Response):
    """The table-level description in a batched describe response.

    Attributes:
        description: The proposed description text.
        confidence: The model's own 0-1 self-assessment of this description.
        rationale: Short model-supplied reason behind ``confidence``.
    """

    description: str = Field(min_length=1, max_length=10000)
    confidence: float = Field(ge=0, le=1)
    rationale: str = Field(max_length=500)


class DescribeResponse(_Response):
    """The complete structure expected from one batched describe call.

    Attributes:
        columns: Descriptions for the requested columns.
        table: The table's own description, when one was requested.
    """

    columns: list[ColumnDescription] = Field(default_factory=list, max_length=1000)
    table: TableDescription | None = None


class DescriptionEngine:
    """Grounded column description generation over a lineage document and catalog.

    Attributes:
        runner: The caller-selected Agent SDK LLM runner; only its ``complete`` API is
            used, so any conforming ``LLMRunnerProtocol`` implementation works.
        model: Explicit SDK model id or registered model slug to request.
        max_tokens: Maximum output tokens per description request.
        override_existing: Whether descriptions this package generated earlier may be
            regenerated. Human-authored text is kept either way.
        warnings: Human-readable notes about columns skipped or rejected during the most
            recent :meth:`run`/:meth:`arun` call.
        summary: Counts from the most recent run — ``generated``, ``inherited``,
            ``skipped_existing`` and ``failed`` — for a caller or CLI to report.
    """

    def __init__(
        self,
        runner: "LLMRunnerProtocol",
        *,
        model: str,
        max_tokens: int = 16000,
        override_existing: bool = False,
    ):
        """Create an engine bound to a runner and model.

        Args:
            runner: The Agent SDK LLM runner to call for each table.
            model: Explicit SDK model id or registered model slug; must be non-blank.
            max_tokens: Maximum output tokens per description request; must be positive.
            override_existing: Regenerate descriptions whose ``description_source`` is
                ``ai`` or ``code``. Human or verified text is never overwritten.

        Raises:
            ValueError: If ``model`` is blank or ``max_tokens`` is not positive.
        """
        if not model.strip():
            raise ValueError("An explicit SDK model ID or registered model slug is required")
        if max_tokens < 1:
            raise ValueError("max_tokens must be positive")
        self.runner = runner
        self.model = model
        self.max_tokens = max_tokens
        self.override_existing = override_existing
        self.warnings = []
        self.summary = {"generated": 0, "inherited": 0, "skipped_existing": 0, "failed": 0}

    def _needs(self, entry):
        """Report whether a catalog table/column entry still needs a description.

        Args:
            entry: A catalog ``tables[]`` or ``schema[]`` dict, or ``None``.

        Returns:
            bool: True when the entry exists and is either undescribed or carries text
            this package generated while ``override_existing`` is set.
        """
        if entry is None:
            return False
        if not entry.get("description"):
            return True
        return self.override_existing and entry.get("description_source") in REGENERATABLE_SOURCES

    def _described(self, entry):
        """Report whether a catalog entry already carries description text.

        Args:
            entry: A catalog ``tables[]`` or ``schema[]`` dict, or ``None``.

        Returns:
            bool: True when the entry exists and has a non-empty description.
        """
        return bool(entry and entry.get("description"))

    def _write(self, entry, proposal):
        """Write a generated description and its attribution onto a catalog entry.

        Args:
            entry: The catalog table/column dict to update in place.
            proposal: A :class:`ColumnDescription` or :class:`TableDescription`.
        """
        entry.update(
            description=proposal.description,
            description_source="ai",
            ai_confidence=proposal.confidence,
            ai_rationale=proposal.rationale,
            ai_model=self.model,
        )

    def run(self, doc, catalog, *, log_dir=None, log_level="INFO", observer=None):
        """Synchronous entry point. Async applications should await ``arun``.

        Args:
            doc: The :class:`~etl_parser.models.LineageDocument` to draw column edges from.
            catalog: The agent catalog dict to enrich with descriptions.
            log_dir: Directory to persist run events and metrics in, when ``observer`` is
                not supplied.
            log_level: Console log level, when ``observer`` is not supplied.
            observer: Explicit observer to use instead of creating one.

        Returns:
            dict: The enriched catalog, as returned by :meth:`arun`. Run counts are left
            on :attr:`summary` for a caller or CLI to report.

        Raises:
            RuntimeError: If called from within a running event loop.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(
                self.arun(doc, catalog, log_dir=log_dir, log_level=log_level, observer=observer)
            )
        raise RuntimeError("An event loop is running; use await engine.arun(doc, catalog)")

    @observed("descriptions")
    async def arun(self, doc, catalog, *, log_dir=None, log_level="INFO", observer=None):
        """Walk columns in topological order and describe each table in one request.

        Columns are visited in dependency order so upstream descriptions exist before
        downstream prompts are built, then grouped into one request per table. A column
        that already has a description is skipped (unless ``override_existing`` allows
        regenerating text this package wrote), and a single-source identity edge with
        exact confidence inherits the upstream description without any call. Columns
        whose lineage is not exact are skipped with a warning. The remaining columns of a
        table, plus the table's own description when it needs one, go out in a single
        request whose response is schema-validated as a :class:`DescribeResponse`; a
        malformed response, a tool request or a non-terminal stop reason is rejected with
        a warning rather than written, and a description for a column that was not
        requested is ignored. Every failure and skip is recorded as an observability
        event; nothing here ever raises to abort the walk.

        Args:
            doc: The :class:`~etl_parser.models.LineageDocument` to draw column edges from.
            catalog: The agent catalog dict to enrich with descriptions. Not mutated; a
                deep copy is enriched and returned.
            log_dir: Unused directly; present for signature parity with :meth:`run`.
            log_level: Unused directly; present for signature parity with :meth:`run`.
            observer: Unused; the active observer is always looked up via
                :func:`~etl_parser.observability.current_observer`.

        Returns:
            dict: A deep copy of ``catalog`` with ``description`` and
            ``description_source`` (``"inherited"`` or ``"ai"``) filled in on the tables
            and columns this call resolved, AI text also carrying ``ai_confidence``,
            ``ai_rationale`` and ``ai_model``. ``self.warnings`` and ``self.summary`` are
            reset and repopulated for this run.

        Raises:
            ImportError: If ``agent_sdk`` is not installed.
        """
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
        self.summary = {"generated": 0, "inherited": 0, "skipped_existing": 0, "failed": 0}
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
        # Tables are visited in the order their first column becomes describable, so a
        # downstream table is never prompted before its upstream descriptions exist.
        grouped = {}
        for target in order:
            grouped.setdefault(target[0], []).append(target)
        domains = {p.code: p.domain for p in doc.products}
        products = {d.id: d.product for d in doc.datasets}
        for dataset_id, targets in grouped.items():
            table = by_table.get(dataset_id)
            pending = []
            for target in targets:
                column = columns.get(target)
                edges = edges_by_target.get(target, [])
                if column is None or not edges or not self._needs(column):
                    reason = "existing_description" if self._described(column) else "no_target"
                    if reason == "existing_description" and edges:
                        # Only a column this run could have described counts as skipped;
                        # a described source column was never a candidate.
                        self.summary["skipped_existing"] += 1
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
                if len(sources) == 1 and all(
                    e.transformation.kind == "identity" and e.provenance.confidence == "exact"
                    for e in edges
                ):
                    upstream = columns.get(next(iter(sources)), {}).get("description")
                    if upstream:
                        column.update(description=upstream, description_source="inherited")
                        self.summary["inherited"] += 1
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
                pending.append((target[1], column, edges))
            # A table description rides along with the columns of that table. Asking
            # about a table with nothing to describe would be a call with no evidence.
            describe_table = table is not None and bool(pending) and self._needs(table)
            if table is None or not pending:
                continue
            known = {
                f"{s.dataset_id}#{s.name}": {
                    "description": columns.get((s.dataset_id, s.name), {}).get("description", ""),
                    "datatype": columns.get((s.dataset_id, s.name), {}).get("datatype"),
                }
                for _, _, edges in pending
                for edge in edges
                for s in edge.sources + edge.indirect_sources
            }
            prompt = build_table_prompt(
                dataset_id,
                table,
                pending,
                describe_table,
                known,
                domains.get(products.get(dataset_id)),
            )
            target = dataset_id
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
                response = DescribeResponse.model_validate_json(
                    "".join(b["text"] for b in completion.content if b.get("type") == "text")
                )
                requested = {name: column for name, column, _ in pending}
                described = set()
                for proposal in response.columns:
                    column = requested.get(proposal.name)
                    if column is None or proposal.name in described:
                        observer.count("descriptions.rejected")
                        observer.event(
                            "description.rejected",
                            level="DEBUG",
                            actor="response_validator",
                            target=(dataset_id, proposal.name),
                            reason="unrequested_or_duplicate_column",
                            request_id=request_id,
                        )
                        continue
                    described.add(proposal.name)
                    self._write(column, proposal)
                    self.summary["generated"] += 1
                    observer.count("descriptions.generated")
                    observer.event(
                        "description.accepted",
                        actor="response_validator",
                        target=(dataset_id, proposal.name),
                        description_digest=digest(proposal.description),
                        reason="nonempty_valid_response",
                        request_id=request_id,
                    )
                if response.table is not None and describe_table:
                    self._write(table, response.table)
                    observer.count("descriptions.tables.generated")
                    observer.event(
                        "description.accepted",
                        actor="response_validator",
                        target=dataset_id,
                        kind="table",
                        description_digest=digest(response.table.description),
                        reason="nonempty_valid_response",
                        request_id=request_id,
                    )
                missing = sorted(set(requested) - described)
                if missing:
                    self.warnings.append(f"No description returned for {dataset_id}: {missing}")
                    observer.count("descriptions.unfilled", len(missing))
                    observer.partial()
            except Exception as exc:
                self.warnings.append(f"Skipped {target}: {type(exc).__name__}")
                if attempted:
                    observer.count("ai.responses.rejected" if completed else "ai.calls.failed")
                self.summary["failed"] += len(pending)
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
        for name, count in self.summary.items():
            observer.gauge(f"descriptions.{name}", count)
        observer.event("description.completed", actor="description_policy", **self.summary)
        return catalog
