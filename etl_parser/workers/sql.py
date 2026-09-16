"""SqlWorker: column-level lineage from SQL text using sqlglot.

Statements are normalised to (target, SELECT) pairs. CREATE TABLE AS, INSERT ... SELECT and
MERGE all become a target dataset plus a select-like body. Each output column is resolved with
``sqlglot.lineage`` to leaf table columns, and columns used only in WHERE, JOIN, GROUP BY or
HAVING are attached as indirect sources.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.lineage import lineage as sqlglot_lineage
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.scope import Scope, build_scope, traverse_scope, walk_in_scope

from etl_parser.identity import dataset_ref_from_id, normalize_dataset_id, split_dataset_id
from etl_parser.models import (
    ColumnEdge,
    ColumnRef,
    DatasetRef,
    Job,
    Provenance,
    TableEdge,
    Transformation,
    Unresolved,
    WorkerResult,
)
from etl_parser.scanner.sinks import ENGINE_DIALECT
from etl_parser.workers.base import (
    comment_schedule,
    job_id_for,
    parse_header,
    repo_relative,
)


class SchemaProvider(Protocol):
    def columns(self, dataset_id: str) -> list[str] | None: ...


class DictSchemaProvider:
    """Schema lookup backed by a ``{db: {table: [cols]}}`` mapping or an agent ``catalog.json``."""

    def __init__(self, source: Mapping | str | Path):
        if isinstance(source, (str, Path)):
            source = json.loads(Path(source).read_text())
        self._cols: dict[str, list[str]] = {}
        if "databases" in source:
            for db in source["databases"]:
                for t in db.get("tables", []):
                    key = f"{db['db_name']}.{t['table_name']}".lower()
                    self._cols[key] = [c["field_name"] for c in t.get("schema", [])]
        else:
            for db, tables in source.items():
                for t, cols in tables.items():
                    self._cols[f"{db}.{t}".lower()] = (
                        list(cols.keys()) if isinstance(cols, Mapping) else list(cols)
                    )

    def columns(self, dataset_id: str) -> list[str] | None:
        _, ns, name = split_dataset_id(dataset_id)
        return self._cols.get(f"{ns}.{name}".lower())


class GlueSchemaProvider:
    """Optional cached Glue lookup, enabled only by an explicit caller choice."""

    def __init__(self, client=None, *, region: str | None = None):
        if client is None:
            import boto3

            client = boto3.client("glue", region_name=region)
        self.client = client
        self._cache: dict[str, list[str] | None] = {}

    def columns(self, dataset_id: str) -> list[str] | None:
        scheme, database, table = split_dataset_id(dataset_id)
        if scheme != "glue":
            return None
        if dataset_id not in self._cache:
            try:
                metadata = self.client.get_table(DatabaseName=database, Name=table)["Table"]
            except self.client.exceptions.EntityNotFoundException:
                self._cache[dataset_id] = None
            else:
                fields = metadata.get("StorageDescriptor", {}).get("Columns", [])
                fields += metadata.get("PartitionKeys", [])
                self._cache[dataset_id] = list(dict.fromkeys(f["Name"] for f in fields))
        return self._cache[dataset_id]


@dataclass
class SqlAnalysis:
    result: WorkerResult
    inputs: set[str] = field(default_factory=set)
    outputs: set[str] = field(default_factory=set)
    output_columns: dict[str, list[str]] = field(default_factory=dict)
    """target dataset id -> ordered output column names (for callers tracking frames)."""


@dataclass
class _Statement:
    expression: exp.Expression
    line_start: int
    line_end: int


class SqlWorker:
    def __init__(self, schema: SchemaProvider | None = None):
        self.schema = schema
        self._temp_tables: dict[str, dict[str, ColumnEdge]] = {}

    # ------------------------------------------------------------------ public
    def analyze_file(
        self, path: Path, *, root: Path | None = None, engine: str = "athena"
    ) -> SqlAnalysis:
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            return SqlAnalysis(
                result=WorkerResult(
                    unresolved=[
                        Unresolved(
                            kind="unsupported_syntax",
                            source_file=repo_relative(path, root),
                            reason=f"read: {exc}",
                            job_id=job_id_for(path, root),
                        )
                    ]
                )
            )
        job_id = job_id_for(path, root)
        header = parse_header(text)
        dialect = ENGINE_DIALECT.get(engine, engine)
        analysis = self.analyze(
            text,
            dialect=dialect,
            engine=engine,
            job_id=job_id,
            source_file=repo_relative(path, root),
        )
        job = Job(
            id=job_id,
            name=path.stem,
            source_file=repo_relative(path, root),
            language="sql",
            engine=engine if engine in {"athena", "spark", "postgres", "mysql"} else "unknown",
            dialect=dialect,
            owner=header.get("owner"),
            description=_sql_description(text),
            inputs=sorted(analysis.inputs),
            outputs=sorted(analysis.outputs),
        )
        if "schedule" in header:
            sched = comment_schedule(job_id, header["schedule"], job.source_file)
            analysis.result.schedules[sched.id] = sched
            job.schedule_id = sched.id
        analysis.result.jobs.append(job)
        return analysis

    def analyze(
        self,
        sql: str,
        *,
        dialect: str = "trino",
        engine: str = "athena",
        job_id: str = "adhoc",
        default_db: str | None = None,
        source_file: str | None = None,
        line_offset: int = 0,
        target_override: str | None = None,
    ) -> SqlAnalysis:
        result = WorkerResult()
        analysis = SqlAnalysis(result=result)
        holes = re.findall(r"\{\{\s*(.*?)\s*\}\}|\$\{([^}]+)\}", sql)
        if holes:
            result.unresolved.append(
                Unresolved(
                    kind="dynamic_sql",
                    source_file=source_file,
                    line=line_offset + 1,
                    reason="Unrendered SQL template requires explicit values",
                    job_id=job_id,
                    partial_text=sql[:1000],
                    symbols=sorted({a or b for a, b in holes}),
                    remediation="Render SQL using explicit scan bindings before analysis.",
                )
            )
            return analysis
        # An analyze call is one SQL session; files must never inherit temp mappings.
        self._temp_tables = {}
        statements = self._parse(sql, dialect, source_file, line_offset, result, job_id)
        for stmt in statements:
            try:
                if isinstance(stmt.expression, exp.Use):
                    default_db = stmt.expression.this.name
                    continue
                self._analyze_statement(
                    stmt,
                    analysis,
                    dialect,
                    engine,
                    job_id,
                    default_db,
                    source_file,
                    target_override,
                )
            except Exception as e:  # Isolate parser/provider failures to this statement.
                result.unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=source_file,
                        line=stmt.line_start,
                        reason=f"{type(e).__name__}: {e}",
                        job_id=job_id,
                    )
                )
        seen = {d.id for d in result.datasets}
        for ds_id in sorted(analysis.inputs | analysis.outputs):
            if ds_id not in seen:
                result.datasets.append(dataset_ref_from_id(ds_id))
        return analysis

    # --------------------------------------------------------------- parsing
    def _parse(
        self,
        sql: str,
        dialect: str,
        source_file: str | None,
        line_offset: int,
        result: WorkerResult,
        job_id: str,
    ) -> list[_Statement]:
        try:
            tokens = sqlglot.tokenize(sql, read=dialect)
        except (SqlglotError, ValueError) as e:
            result.unresolved.append(
                Unresolved(
                    kind="unsupported_syntax",
                    source_file=source_file,
                    line=line_offset + 1,
                    reason=f"tokenize: {e}",
                    partial_text=sql[:200],
                    job_id=job_id,
                )
            )
            return []
        # Token boundaries preserve quoted semicolons, offsets and empty statements.
        chunks: list[list] = []
        chunk: list = []
        for tok in tokens:
            if tok.token_type == sqlglot.TokenType.SEMICOLON:
                if chunk:
                    chunks.append(chunk)
                    chunk = []
            else:
                chunk.append(tok)
        if chunk:
            chunks.append(chunk)

        statements: list[_Statement] = []
        for chunk in chunks:
            start, end = chunk[0].start, chunk[-1].end + 1
            ls = sql.count("\n", 0, start) + 1
            le = sql.count("\n", 0, end) + 1
            text = sql[start:end]
            try:
                parsed = sqlglot.parse(text, read=dialect)
            except SqlglotError as e:
                result.unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=source_file,
                        line=line_offset + ls,
                        reason=f"parse: {str(e).splitlines()[0]}",
                        partial_text=text[:200],
                        job_id=job_id,
                    )
                )
                continue
            for expression in parsed:
                if expression is not None:
                    statements.append(_Statement(expression, line_offset + ls, line_offset + le))
        return statements

    # -------------------------------------------------------------- analysis
    def _analyze_statement(
        self,
        stmt: _Statement,
        analysis: SqlAnalysis,
        dialect: str,
        engine: str,
        job_id: str,
        default_db: str | None,
        source_file: str | None,
        target_override: str | None,
    ) -> None:
        e = stmt.expression
        result = analysis.result
        norm = lambda name: normalize_dataset_id(name, engine=engine, default_db=default_db)  # noqa: E731

        if isinstance(e, exp.Use):
            return
        if isinstance(e, exp.Drop):
            if isinstance(e.this, exp.Table):
                self._temp_tables.pop(norm(_table_name(e.this)), None)
            return
        if isinstance(e, (exp.Alter, exp.Set, exp.Pragma)):
            return
        if isinstance(e, exp.Merge):
            self._analyze_merge(e, stmt, analysis, dialect, job_id, norm, source_file)
            return
        if isinstance(e, exp.Update):
            self._analyze_update(e, stmt, analysis, dialect, job_id, norm, source_file)
            return

        target: str | None = target_override
        body: exp.Expression | None = None
        is_temp = False
        if isinstance(e, exp.Create):
            table = e.this.this if isinstance(e.this, exp.Schema) else e.this
            if isinstance(table, exp.Table) and e.kind in {"TABLE", "VIEW"}:
                target = target or norm(_table_name(table))
                body = e.expression
                is_temp = any(
                    isinstance(p, exp.TemporaryProperty)
                    for p in (e.args.get("properties") or exp.Properties()).expressions
                ) or bool(e.args.get("temporary"))
                if body is None and isinstance(e.this, exp.Schema):
                    # Plain DDL: register columns, no lineage.
                    cols = [c.name for c in e.this.expressions if isinstance(c, exp.ColumnDef)]
                    result.datasets.append(
                        DatasetRef(
                            id=target,
                            namespace=dataset_ref_from_id(target).namespace,
                            name=dataset_ref_from_id(target).name,
                            columns=cols,
                        )
                    )
                    analysis.outputs.add(target)
                    return
        elif isinstance(e, exp.Insert):
            table = e.this.this if isinstance(e.this, exp.Schema) else e.this
            if isinstance(table, exp.Table):
                target = target or norm(_table_name(table))
            body = e.expression
            if isinstance(body, exp.Values):
                if target:
                    analysis.outputs.add(target)
                return
        elif isinstance(e, (exp.Select, exp.SetOperation, exp.Subquery)):
            body = e
        elif isinstance(e, exp.Delete):
            table = e.this
            if isinstance(table, exp.Table):
                ds = norm(_table_name(table))
                analysis.inputs.add(ds)
                analysis.outputs.add(ds)
            return
        else:
            result.unresolved.append(
                Unresolved(
                    kind="unsupported_syntax",
                    source_file=source_file,
                    line=stmt.line_start,
                    reason=f"statement type {type(e).__name__} not modelled",
                    partial_text=e.sql(dialect=dialect)[:200],
                    job_id=job_id,
                )
            )
            return

        if body is None:
            return
        if isinstance(body, exp.Subquery):
            body = body.this

        body = body.copy()
        # WITH can belong to INSERT itself, outside its SELECT expression.
        if e is not body and e.args.get("with_") and not body.args.get("with_"):
            body.set("with_", e.args["with_"].copy())

        target_columns = []
        if isinstance(e, (exp.Insert, exp.Create)) and isinstance(e.this, exp.Schema):
            target_columns = [c.name for c in e.this.expressions]

        self._analyze_select(
            body,
            target,
            stmt,
            analysis,
            dialect,
            engine,
            job_id,
            norm,
            source_file,
            is_temp,
            target_columns,
        )

    def _analyze_select(
        self,
        body: exp.Expression,
        target: str | None,
        stmt: _Statement,
        analysis: SqlAnalysis,
        dialect: str,
        engine: str,
        job_id: str,
        norm,
        source_file: str | None,
        is_temp: bool,
        target_columns: list[str] | None = None,
    ) -> None:
        result = analysis.result
        prov = Provenance(parser="sqlglot", dialect=dialect)

        # Real tables referenced (CTE names excluded).
        source_tables: dict[str, str] = {}  # sqlglot table sql -> dataset id
        for scope in traverse_scope(body):
            for _, t in scope.selected_sources.values():
                if isinstance(t, exp.Table) and t.name:
                    ds = norm(_table_name(t))
                    source_tables[_table_name(t)] = ds
                    analysis.inputs.add(ds)
        if target:
            analysis.outputs.add(target)

        schema_map = self._schema_map(source_tables, norm)
        try:
            qualified = qualify(
                body.copy(),
                schema=schema_map or None,
                dialect=dialect,
                validate_qualify_columns=False,
                infer_schema=True,
                allow_partial_qualification=True,
            )
        except SqlglotError as e:
            result.unresolved.append(
                Unresolved(
                    kind="unsupported_syntax",
                    source_file=source_file,
                    line=stmt.line_start,
                    reason=f"qualify: {str(e).splitlines()[0]}",
                    job_id=job_id,
                )
            )
            if target:
                for ds in sorted(set(source_tables.values())):
                    result.table_edges.append(
                        TableEdge(
                            source=ds,
                            target=target,
                            provenance=prov.model_copy(update={"confidence": "partial"}),
                            job_id=job_id,
                            source_file=source_file,
                            line=stmt.line_start,
                        )
                    )
            return

        # Check after qualification: derived-table stars can expand without a catalog,
        # while a partially supplied catalog can still leave unresolved stars.
        has_star = any(
            p.is_star for scope in traverse_scope(qualified) for p in scope.expression.selects
        )
        if has_star:
            result.unresolved.append(
                Unresolved(
                    kind="missing_schema",
                    source_file=source_file,
                    line=stmt.line_start,
                    reason="SELECT * could not be fully expanded with the available schema",
                    job_id=job_id,
                )
            )
            prov = prov.model_copy(update={"confidence": "partial"})

        indirect = self._indirect_sources(qualified, norm, dialect)
        outer = qualified.this if isinstance(qualified, exp.Subquery) else qualified
        selects = outer.selects if isinstance(outer, (exp.Select, exp.SetOperation)) else []
        if target_columns and (has_star or len(target_columns) != len(selects)):
            result.unresolved.append(
                Unresolved(
                    kind="unknown_column",
                    source_file=source_file,
                    line=stmt.line_start,
                    reason="Target column count cannot be matched to SELECT projections",
                    job_id=job_id,
                )
            )
            selects = []
        out_cols: list[str] = []
        edges_for_target: dict[str, ColumnEdge] = {}
        for position, proj in enumerate(selects):
            name = proj.alias_or_name
            if not name or proj.is_star:
                continue
            output_name = target_columns[position] if target_columns else name
            out_cols.append(output_name)
            if not target:
                continue
            edge = self._column_edge(
                name,
                proj,
                qualified,
                schema_map,
                dialect,
                norm,
                target,
                job_id,
                indirect,
                prov,
                stmt,
                source_file,
                result,
            )
            if edge is not None:
                edge.target.name = output_name
                edge = self._expand_temp(edge)
                result.column_edges.append(edge)
                edges_for_target[output_name] = edge

        if target:
            analysis.output_columns[target] = out_cols
            ds_ref = dataset_ref_from_id(target)
            result.datasets.append(ds_ref.model_copy(update={"columns": out_cols}))
            for ds in sorted(set(source_tables.values())):
                result.table_edges.append(
                    TableEdge(
                        source=ds,
                        target=target,
                        provenance=prov,
                        job_id=job_id,
                        source_file=source_file,
                        line=stmt.line_start,
                    )
                )
            if is_temp:
                self._temp_tables[target] = edges_for_target
        else:
            analysis.output_columns["__select__"] = out_cols

    def _column_edge(
        self,
        name,
        proj,
        qualified,
        schema_map,
        dialect,
        norm,
        target,
        job_id,
        indirect,
        prov,
        stmt,
        source_file,
        result,
    ) -> ColumnEdge | None:
        try:
            node = sqlglot_lineage(
                exp.column(name, quoted=True),
                qualified,
                schema=schema_map or None,
                dialect=dialect,
                scope=build_scope(qualified),
            )
        except SqlglotError as e:
            result.unresolved.append(
                Unresolved(
                    kind="unknown_column",
                    source_file=source_file,
                    line=stmt.line_start,
                    reason=f"lineage({name}): {str(e).splitlines()[0]}",
                    job_id=job_id,
                )
            )
            return ColumnEdge(
                target=ColumnRef(dataset_id=target, name=name),
                transformation=Transformation(
                    expression=proj.unalias().sql(dialect=dialect),
                    kind=_kind(proj),
                    source_file=source_file,
                    line_start=stmt.line_start,
                    line_end=stmt.line_end,
                ),
                provenance=prov.model_copy(update={"confidence": "partial"}),
                job_id=job_id,
            )
        sources: list[ColumnRef] = []
        confidence = prov.confidence
        seen = set()
        for leaf in node.walk():
            if leaf.downstream:
                continue
            src = leaf.source
            if isinstance(src, exp.Table) and src.name:
                col = exp.to_column(leaf.name)
                ref = ColumnRef(dataset_id=norm(_table_name(src)), name=col.name)
                known = self.schema.columns(ref.dataset_id) if self.schema else None
                if known is not None and ref.name not in known:
                    confidence = "partial"
                    result.unresolved.append(
                        Unresolved(
                            kind="unknown_column",
                            source_file=source_file,
                            line=stmt.line_start,
                            reason=f"Column {ref.name!r} absent from schema: {ref.dataset_id}",
                            job_id=job_id,
                        )
                    )
                    continue
                if (ref.dataset_id, ref.name) not in seen:
                    seen.add((ref.dataset_id, ref.name))
                    sources.append(ref)
            elif not isinstance(src, exp.Placeholder) and not list(
                leaf.expression.find_all(exp.Column)
            ):
                pass  # Constants, COUNT(*) and zero-argument functions have no column sources.
            elif isinstance(src, exp.Table) or leaf.reference_node_name:
                confidence = "partial"
            elif leaf is not node or not isinstance(
                proj.unalias(),
                (
                    exp.Literal,
                    exp.Null,
                    exp.Boolean,
                    exp.Anonymous,
                    exp.CurrentTimestamp,
                    exp.CurrentDate,
                ),
            ):
                # Column could not be traced to a table (missing schema or unknown alias).
                confidence = "partial"
                result.unresolved.append(
                    Unresolved(
                        kind="unknown_column",
                        source_file=source_file,
                        line=stmt.line_start,
                        reason=f"could not resolve source of {leaf.name!r} for output {name!r}",
                        job_id=job_id,
                    )
                )
        kind = _kind(proj)
        indirect_refs = _dedup(
            r for k in ("filter", "join", "aggregation") for r in indirect.get(k, [])
        )
        return ColumnEdge(
            target=ColumnRef(dataset_id=target, name=name),
            sources=sources,
            indirect_sources=indirect_refs,
            transformation=Transformation(
                expression=proj.unalias().sql(dialect=dialect),
                kind=kind,
                source_file=source_file,
                line_start=stmt.line_start,
                line_end=stmt.line_end,
            ),
            provenance=prov.model_copy(update={"confidence": confidence}),
            job_id=job_id,
        )

    def _expand_temp(self, edge: ColumnEdge) -> ColumnEdge:
        """Replace sources that point at a session temp table with that table's own sources."""
        if not self._temp_tables:
            return edge
        new_sources: list[ColumnRef] = []
        indirect: list[ColumnRef] = []
        confidence = edge.provenance.confidence
        for s in edge.sources:
            upstream = self._temp_tables.get(s.dataset_id, {}).get(s.name)
            if upstream is not None:
                new_sources.extend(upstream.sources)
                indirect.extend(upstream.indirect_sources)
                if upstream.provenance.confidence != "exact":
                    confidence = upstream.provenance.confidence
            else:
                new_sources.append(s)
        for s in edge.indirect_sources:
            upstream = self._temp_tables.get(s.dataset_id, {}).get(s.name)
            indirect.extend(upstream.sources + upstream.indirect_sources if upstream else [s])
            if upstream and upstream.provenance.confidence != "exact":
                confidence = upstream.provenance.confidence
        return edge.model_copy(
            update={
                "sources": _dedup(new_sources),
                "indirect_sources": _dedup(indirect),
                "provenance": edge.provenance.model_copy(update={"confidence": confidence}),
            }
        )

    def _analyze_merge(self, e: exp.Merge, stmt, analysis, dialect, job_id, norm, source_file):
        target = norm(_table_name(e.this))
        analysis.inputs.add(target)
        analysis.outputs.add(target)
        merged: dict[str, ColumnEdge] = {}
        for when in e.args["whens"].expressions:
            action = when.args.get("then")
            pairs = []
            if isinstance(action, exp.Update):
                pairs = [
                    (eq.this.name, eq.expression)
                    for eq in action.expressions
                    if isinstance(eq, exp.EQ) and isinstance(eq.this, exp.Column)
                ]
            elif isinstance(action, exp.Insert):
                cols = action.this.expressions if isinstance(action.this, exp.Tuple) else []
                values = action.expression
                vals = (
                    values.expressions[0].expressions
                    if isinstance(values, exp.Values) and values.expressions
                    else values.expressions
                    if isinstance(values, exp.Tuple)
                    else []
                )
                if not cols or len(cols) != len(vals):
                    analysis.result.unresolved.append(
                        Unresolved(
                            kind="missing_schema",
                            source_file=source_file,
                            line=stmt.line_start,
                            reason="MERGE INSERT requires a matching explicit target column list",
                            job_id=job_id,
                        )
                    )
                    continue
                pairs = [(c.name, v) for c, v in zip(cols, vals, strict=True)]
            if not pairs:
                continue
            query = exp.select(
                *[exp.alias_(value.copy(), name, quoted=True) for name, value in pairs]
            ).from_(e.this.copy())
            query = query.join(e.args["using"].copy(), on=e.args["on"].copy())
            if when.args.get("condition") is not None:
                query = query.where(when.args["condition"].copy())
            if e.args.get("with_"):
                query.set("with_", e.args["with_"].copy())
            branch = SqlAnalysis(result=WorkerResult())
            self._analyze_select(
                query, target, stmt, branch, dialect, "unknown", job_id, norm, source_file, False
            )
            analysis.inputs.update(branch.inputs)
            for edge in branch.result.column_edges:
                prior = merged.get(edge.target.name)
                if prior:
                    prior.sources = _dedup(prior.sources + edge.sources)
                    prior.indirect_sources = _dedup(prior.indirect_sources + edge.indirect_sources)
                    # Preserve branch semantics in the original MERGE, not a synthetic CASE.
                    prior.transformation.expression = e.sql(dialect=dialect)
                    if prior.transformation.kind != edge.transformation.kind:
                        prior.transformation.kind = "expression"
                    if edge.provenance.confidence != "exact":
                        prior.provenance.confidence = edge.provenance.confidence
                else:
                    merged[edge.target.name] = edge
            branch.result.column_edges = []
            analysis.result.extend(branch.result)
        analysis.result.column_edges.extend(merged.values())
        analysis.output_columns[target] = list(merged)

    def _analyze_update(self, e: exp.Update, stmt, analysis, dialect, job_id, norm, source_file):
        target = norm(_table_name(e.this))
        projections = [
            exp.alias_(eq.expression.copy(), eq.this.name, quoted=True)
            for eq in e.expressions
            if isinstance(eq, exp.EQ) and isinstance(eq.this, exp.Column)
        ]
        query = exp.select(*projections).from_(e.this.copy())
        if e.args.get("from_"):
            query = query.join(e.args["from_"].this.copy(), join_type="CROSS")
        for key in ("where", "with_"):
            if e.args.get(key):
                query.set(key, e.args[key].copy())
        self._analyze_select(
            query, target, stmt, analysis, dialect, "unknown", job_id, norm, source_file, False
        )

    # --------------------------------------------------------------- helpers
    def _schema_map(self, source_tables: Mapping[str, str], norm) -> dict:
        out: dict = {}
        for table_sql, ds in source_tables.items():
            cols = (
                list(self._temp_tables[ds])
                if ds in self._temp_tables
                else (self.schema.columns(ds) if self.schema else None)
            )
            if not cols:
                continue
            parts = [p.sql() for p in exp.to_table(table_sql).parts]
            node = out
            for p in parts[:-1]:
                node = node.setdefault(p, {})
            node[parts[-1]] = {c: "unknown" for c in cols}
        return out

    def _indirect_sources(self, qualified, norm, dialect) -> dict[str, list[ColumnRef]]:
        result: dict[str, list[ColumnRef]] = {"filter": [], "join": [], "aggregation": []}
        for scope in traverse_scope(qualified):
            query = scope.expression
            if not isinstance(query, exp.Select):
                continue
            clauses = [
                ("filter", query.args.get("where")),
                ("filter", query.args.get("having")),
                ("filter", query.args.get("qualify")),
                ("aggregation", query.args.get("group")),
                *[("join", j.args.get("on")) for j in query.args.get("joins", [])],
            ]
            for kind, clause in clauses:
                if clause is None:
                    continue
                for col in walk_in_scope(clause):
                    if not isinstance(col, exp.Column):
                        continue
                    source = scope.sources.get(col.table)
                    if isinstance(source, exp.Table):
                        result[kind].append(
                            ColumnRef(
                                dataset_id=norm(_table_name(source)),
                                name=col.name,
                            )
                        )
                    elif isinstance(source, Scope):
                        node = sqlglot_lineage(
                            exp.column(col.name, quoted=True),
                            source.expression,
                            scope=source,
                            dialect=dialect,
                        )
                        for leaf in node.walk():
                            if not leaf.downstream and isinstance(leaf.source, exp.Table):
                                result[kind].append(
                                    ColumnRef(
                                        dataset_id=norm(_table_name(leaf.source)),
                                        name=exp.to_column(leaf.name).name,
                                    )
                                )
        return {k: _dedup(v) for k, v in result.items()}


def _dedup(refs: Iterable[ColumnRef]) -> list[ColumnRef]:
    seen = set()
    out = []
    for r in refs:
        if (r.dataset_id, r.name) not in seen:
            seen.add((r.dataset_id, r.name))
            out.append(r)
    return out


def _table_name(t: exp.Table) -> str:
    return ".".join(p.sql() for p in t.parts)


def _all_selects(body: exp.Expression) -> list[exp.Expression]:
    if isinstance(body, exp.SetOperation):
        return _all_selects(body.left) + _all_selects(body.right)
    if isinstance(body, exp.Subquery):
        return _all_selects(body.this)
    if isinstance(body, exp.Select):
        return list(body.selects)
    return []


def _kind(proj: exp.Expression) -> str:
    inner = proj.unalias() if isinstance(proj, exp.Alias) else proj
    if inner.find(exp.Window):
        return "window"
    if inner.find(exp.AggFunc):
        return "aggregation"
    if isinstance(inner, exp.Column):
        return "identity"
    if isinstance(inner, (exp.Literal, exp.Null, exp.Boolean)):
        return "expression"
    return "expression"


def _sql_description(text: str) -> str | None:
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("--"):
            body = s.lstrip("-").strip()
            if body and ":" not in body.split(" ")[0]:
                return body
        elif s:
            break
    return None
