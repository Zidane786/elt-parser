"""SqlWorker: column-level lineage from SQL text using sqlglot.

Implements design section 8.1. Statements are normalised to ``(target, SELECT)`` pairs:
``CREATE TABLE AS``, ``INSERT ... SELECT``, ``INSERT OVERWRITE``, ``MERGE``, and ``UPDATE``
all become a target dataset plus a select-like body; a plain ``SELECT`` has no target and
yields inputs only. Each output column is resolved with ``sqlglot.lineage`` to leaf table
columns, and columns used only in ``WHERE``, ``JOIN ... ON``, ``GROUP BY``, or ``HAVING``
are attached as indirect sources with kind ``filter``, ``join``, or ``aggregation``. A
session-scoped temp table map lets a temp table created in one statement be expanded when
read by a later statement in the same ``analyze``/``analyze_file`` call.

Entry points: ``SqlWorker.analyze`` (SQL text, for embedded/dynamic SQL) and
``SqlWorker.analyze_file`` (a ``.sql`` file, builds the ``Job`` around it). Schema lookup is
pluggable through the ``SchemaProvider`` protocol, with ``DictSchemaProvider`` (a
``catalog.json`` or plain mapping) and ``GlueSchemaProvider`` (boto3) implementations.
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
    JoinCondition,
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
    """Pluggable schema lookup used by ``SqlWorker`` to qualify columns and star-expand."""

    def columns(self, dataset_id: str) -> list[str] | None:
        """Return the known column names of a dataset, or ``None`` when the schema is unknown.

        Args:
            dataset_id: Canonical dataset id (see ``identity.py``).

        Returns:
            Ordered column names, or ``None`` if the dataset is not present in this
            provider's schema.
        """
        ...


class DictSchemaProvider:
    """Schema lookup backed by a ``{db: {table: [cols]}}`` mapping or an agent ``catalog.json``."""

    def __init__(self, source: Mapping | str | Path):
        """Load a schema from an in-memory mapping, a JSON file path, or a ``catalog.json``.

        Args:
            source: Either a ``Mapping`` (a ``catalog.json``-shaped dict with a
                ``"databases"`` key, or a plain ``{db: {table: [cols]}}``/``{db: {table:
                {col: type}}}`` mapping), or a ``str``/``Path`` to a JSON file containing
                one of those shapes.

        Raises:
            ValueError: If ``source`` (or the parsed JSON file) is not a mapping, if a
                database entry does not map table names to column lists, or if a table's
                columns are not a list of non-empty strings.
        """
        if isinstance(source, (str, Path)):
            source = json.loads(Path(source).read_text())
        if not isinstance(source, Mapping):
            raise ValueError("Schema must be a mapping of databases to table column lists")
        self._cols: dict[str, list[str]] = {}
        if "databases" in source:
            for db in source["databases"]:
                for t in db.get("tables", []):
                    key = f"{db['db_name']}.{t['table_name']}".lower()
                    self._cols[key] = [c["field_name"] for c in t.get("schema", [])]
        else:
            for db, tables in source.items():
                if not isinstance(tables, Mapping):
                    raise ValueError(f"Schema database {db!r} must map tables to column lists")
                for t, cols in tables.items():
                    if not isinstance(cols, (list, tuple, Mapping)) or not all(
                        isinstance(c, str) and c for c in cols
                    ):
                        raise ValueError(f"Schema {db}.{t}: expected a list of column names")
                    self._cols[f"{db}.{t}".lower()] = (
                        list(cols.keys()) if isinstance(cols, Mapping) else list(cols)
                    )

    def columns(self, dataset_id: str) -> list[str] | None:
        """Return the columns for a dataset id, matched by lower-cased ``namespace.name``.

        Args:
            dataset_id: Canonical dataset id.

        Returns:
            The dataset's column names, or ``None`` when it is not in the loaded schema.
        """
        _, ns, name = split_dataset_id(dataset_id)
        return self._cols.get(f"{ns}.{name}".lower())


class GlueSchemaProvider:
    """Optional cached Glue lookup, enabled only by an explicit caller choice."""

    def __init__(self, client=None, *, region: str | None = None):
        """Create a Glue-backed schema provider.

        Args:
            client: A boto3 Glue client, or ``None`` to create one with ``boto3.client
                ("glue", region_name=region)``.
            region: AWS region for the default client. Ignored when ``client`` is given.
        """
        if client is None:
            import boto3

            client = boto3.client("glue", region_name=region)
        self.client = client
        self._cache: dict[str, list[str] | None] = {}

    def columns(self, dataset_id: str) -> list[str] | None:
        """Return a Glue table's columns (storage columns plus partition keys), cached.

        Args:
            dataset_id: Canonical dataset id; only ``glue://`` ids are looked up.

        Returns:
            Deduplicated column names in Glue's reported order, or ``None`` when
            ``dataset_id`` is not a ``glue`` scheme dataset or Glue has no such table.
        """
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
    """Result of analysing one SQL text: the ``WorkerResult`` plus dataset-level summary.

    Returned by ``SqlWorker.analyze``/``analyze_file`` so callers such as PythonWorker's
    DataFrame tracker can see which datasets were touched and what columns a target ended
    up with, without re-deriving them from ``result``.

    Attributes:
        result: Datasets, jobs, column/table edges, and unresolved items produced.
        inputs: Dataset ids read by any statement in the analysed text.
        outputs: Dataset ids written by any statement in the analysed text.
        output_columns: Target dataset id -> ordered output column names, keyed
            ``"__select__"`` for a plain ``SELECT`` with no target (for callers tracking
            frames).
    """

    result: WorkerResult
    inputs: set[str] = field(default_factory=set)
    outputs: set[str] = field(default_factory=set)
    output_columns: dict[str, list[str]] = field(default_factory=dict)

    @property
    def join_conditions(self) -> list[JoinCondition]:
        """The ``JOIN ... ON`` equalities observed in the analysed text.

        Returns:
            ``result.join_conditions``: deduplicated and sorted ``JoinCondition``
            observations, each resolved to physical columns, for relation inference by
            the catalog exporter.
        """
        return self.result.join_conditions


@dataclass
class _Statement:
    """One parsed SQL statement together with its line span in the original source text.

    Attributes:
        expression: The parsed sqlglot expression for the statement.
        line_start: 1-based line number where the statement starts.
        line_end: 1-based line number where the statement ends.
        holes: Names of the ``{{ ... }}``/``${...}`` template placeholders found in the
            statement that were kept as literal values (design section 8.2 step 3). A
            non-empty list downgrades every edge of the statement to ``partial``.
        hole_line: Line of the first placeholder, where the ``dynamic_sql`` note is
            reported.
        text: The statement's original source text (before placeholder substitution).
    """

    expression: exp.Expression
    line_start: int
    line_end: int
    holes: list[str] = field(default_factory=list)
    hole_line: int | None = None
    text: str = ""


_HOLE = re.compile(r"\{\{\s*(.*?)\s*\}\}|\$\{([^}]*)\}", re.S)
_HOLE_MARKER = "__etl_hole_"
_HOLE_TOKEN = re.compile(rf"{_HOLE_MARKER}(\d+)__")


def _substitute_holes(text: str) -> tuple[str, list[tuple[str, str, int]]]:
    """Replace template placeholders in one statement with parseable marker identifiers.

    Args:
        text: The statement's source text, possibly containing ``{{ name }}`` (Jinja) or
            ``${NAME}`` (shell) placeholders.

    Returns:
        The substituted text and one ``(marker, name, offset)`` tuple per placeholder,
        where ``marker`` is the identifier that replaced it, ``name`` the placeholder's
        inner text (``"?"`` when empty) and ``offset`` its character offset in ``text``.
    """
    holes: list[tuple[str, str, int]] = []

    def replace(match: re.Match) -> str:
        marker = f"{_HOLE_MARKER}{len(holes)}__"
        name = (match.group(1) if match.group(1) is not None else match.group(2)) or "?"
        holes.append((marker, name.strip() or "?", match.start()))
        return marker

    return _HOLE.sub(replace, text), holes


def _place_holes(expression: exp.Expression, holes: list[tuple[str, str, int]]) -> bool:
    """Decide whether a statement's placeholders sit in identity or value positions.

    A marker inside a string literal or standing alone as a bare value in a predicate
    (``WHERE amount > {{ threshold }}``) does not affect which tables or columns the
    statement touches: the marker is rewritten back to a string literal of the original
    placeholder text so the statement can be analysed with ``partial`` confidence. A
    marker that names a table part, appears in a projection, or acts as an alias would
    change identity, so the statement must stay ``dynamic_sql``.

    Args:
        expression: The parsed statement whose identifiers/literals carry markers. It is
            modified in place when every marker is in a value position.
        holes: The ``(marker, name, offset)`` tuples from ``_substitute_holes``.

    Returns:
        ``True`` when at least one marker sits in an identity position (the statement
        must not be analysed), ``False`` when all markers were rewritten to literals.
    """
    names = {marker: f"{{{{ {name} }}}}" for marker, name, _ in holes}

    def restore(text: str) -> str:
        return _HOLE_TOKEN.sub(lambda m: names.get(m.group(0), m.group(0)), text)

    rewrites: list[tuple[exp.Expression, exp.Expression]] = []
    for node in expression.walk():
        if isinstance(node, exp.Literal) and node.is_string and _HOLE_TOKEN.search(node.this):
            rewrites.append((node, exp.Literal.string(restore(node.this))))
        elif isinstance(node, exp.Identifier) and _HOLE_TOKEN.search(node.this):
            parent = node.parent
            if not isinstance(parent, exp.Column) or parent.args.get("table") is not None:
                return True
            select = parent.find_ancestor(exp.Select)
            if select is None or any(parent is n for p in select.expressions for n in p.walk()):
                return True
            rewrites.append((parent, exp.Literal.string(restore(node.this))))
    for old, new in rewrites:
        old.replace(new)
    return False


class SqlWorker:
    """Parses SQL text with sqlglot and produces column- and table-level lineage.

    Implements design section 8.1. One instance carries a session-scoped temp table map
    (``_temp_tables``) that is reset at the start of every ``analyze`` call, so a temp
    table created in one statement can be expanded when read by a later statement in the
    same call, but never leaks across unrelated files or calls.

    Attributes:
        schema: Optional ``SchemaProvider`` used to qualify columns, expand ``SELECT *``,
            and validate that resolved columns actually exist.
    """

    def __init__(self, schema: SchemaProvider | None = None):
        """Create a worker, optionally backed by a schema provider.

        Args:
            schema: Schema lookup used for qualification and star-expansion, or ``None``
                to analyze without one (stars and unqualified columns stay unresolved).
        """
        self.schema = schema
        self._temp_tables: dict[str, dict[str, ColumnEdge]] = {}
        # Temp dataset id -> physical datasets it was built from (for table edges/inputs).
        self._temp_sources: dict[str, set[str]] = {}

    # ------------------------------------------------------------------ public
    def analyze_file(
        self, path: Path, *, root: Path | None = None, engine: str = "athena"
    ) -> SqlAnalysis:
        """Analyze a ``.sql`` file and build the ``Job`` that represents it.

        Reads the file, extracts its header (``owner``, ``schedule``, ...) via
        ``parse_header``, analyzes the SQL body, and appends a ``Job`` (and, when the
        header declares a schedule, a ``Schedule``) to the returned result.

        Args:
            path: Path to the ``.sql`` file.
            root: Repo root used to compute the job id and repo-relative source file path.
            engine: Executing engine (``athena``, ``spark``, ``postgres``, ``mysql``, ...),
                used to pick the SQL dialect via ``ENGINE_DIALECT`` and to set
                ``Job.engine``/``Job.dialect``.

        Returns:
            The ``SqlAnalysis`` for the file's SQL, with its ``result.jobs`` containing the
            file's ``Job``. When the file cannot be read, ``result.unresolved`` holds a
            single ``unsupported_syntax`` item describing the read error and no job is
            added.
        """
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
        """Analyze one SQL session's text and return its lineage.

        Splits ``sql`` into statements, tracks a ``USE`` default database, and dispatches
        each statement to the merge, update, or generic (create/insert/select/delete)
        analysis path. Resets the temp table map first, since a call to ``analyze``
        represents one self-contained SQL session.

        Args:
            sql: The SQL text to analyze. May contain multiple ``;``-separated statements.
            dialect: sqlglot dialect to parse and qualify with.
            engine: Executing engine, used only to normalise dataset ids (see
                ``identity.normalize_dataset_id``).
            job_id: Job id attached to every edge and unresolved item produced.
            default_db: Default database for one-part table names, overridden by any
                ``USE`` statement encountered.
            source_file: Repo-relative source file path recorded on edges and unresolved
                items.
            line_offset: Line number to add to every reported line, for SQL embedded in a
                Python file at a known offset.
            target_override: When given, used as every statement's target dataset instead
                of the one derived from the statement (for SQL embedded in a call whose
                target is known some other way).

        Returns:
            The ``SqlAnalysis`` for ``sql``. If ``sql`` contains an unrendered
            ``{{...}}``/``${...}`` template placeholder, analysis stops immediately and
            ``result.unresolved`` holds a single ``dynamic_sql`` item; otherwise a failure
            analyzing one statement is isolated to that statement's ``unresolved`` item and
            the rest of the statements are still analyzed.
        """
        result = WorkerResult()
        analysis = SqlAnalysis(result=result)
        if "{%" in sql:
            result.unresolved.append(
                Unresolved(
                    kind="dynamic_sql",
                    source_file=source_file,
                    line=line_offset + 1,
                    reason="Template control flow ({% ... %}) requires rendering first",
                    job_id=job_id,
                    partial_text=sql[:1000],
                    remediation="Render SQL using explicit scan bindings before analysis.",
                )
            )
            return analysis
        # An analyze call is one SQL session; files must never inherit temp mappings.
        self._temp_tables = {}
        self._temp_sources = {}
        statements = self._parse(sql, dialect, source_file, line_offset, result, job_id)
        for stmt in statements:
            try:
                if isinstance(stmt.expression, exp.Use):
                    default_db = stmt.expression.this.name
                    continue
                if stmt.holes:
                    result.unresolved.append(
                        Unresolved(
                            kind="dynamic_sql",
                            source_file=source_file,
                            line=stmt.hole_line,
                            reason="Template placeholder treated as a literal value; "
                            "statement analysed with partial confidence",
                            job_id=job_id,
                            partial_text=stmt.text[:1000],
                            symbols=list(stmt.holes),
                            remediation="Render SQL using explicit scan bindings before analysis.",
                        )
                    )
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
        unique = {j.model_dump_json(): j for j in result.join_conditions}
        result.join_conditions = [
            unique[k]
            for k in sorted(
                unique,
                key=lambda k: (
                    unique[k].left.dataset_id,
                    unique[k].left.name,
                    unique[k].right.dataset_id,
                    unique[k].right.name,
                    k,
                ),
            )
        ]
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
        """Tokenize and split ``sql`` into individually-parsed statements.

        Splitting on tokenized semicolons (rather than a naive string split) preserves
        quoted semicolons, keeps accurate character offsets for line numbers, and skips
        empty statements.

        Args:
            sql: The full SQL text.
            dialect: sqlglot dialect to tokenize and parse with.
            source_file: Recorded on any ``unsupported_syntax`` item raised.
            line_offset: Added to every reported line number.
            result: Worker result that tokenize/parse failures are appended to.
            job_id: Recorded on any ``unsupported_syntax`` item raised.

        Returns:
            One ``_Statement`` per successfully parsed chunk. A chunk that fails to parse
            contributes an ``unsupported_syntax`` item to ``result`` and is skipped, so the
            fatal ``tokenize`` failure aside, a parse error in one statement does not stop
            the others from being returned.
        """
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
            substituted, holes = _substitute_holes(text)
            hole_line = line_offset + ls + text.count("\n", 0, holes[0][2]) if holes else None
            try:
                parsed = sqlglot.parse(substituted, read=dialect)
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
                if expression is None:
                    continue
                if holes and _place_holes(expression, holes):
                    result.unresolved.append(
                        Unresolved(
                            kind="dynamic_sql",
                            source_file=source_file,
                            line=hole_line,
                            reason="Template placeholder names a table, column or alias",
                            job_id=job_id,
                            partial_text=text[:1000],
                            symbols=sorted({name for _, name, _ in holes}),
                            remediation="Render SQL using explicit scan bindings before analysis.",
                        )
                    )
                    continue
                statements.append(
                    _Statement(
                        expression,
                        line_offset + ls,
                        line_offset + le,
                        holes=sorted({name for _, name, _ in holes}),
                        hole_line=hole_line,
                        text=text,
                    )
                )
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
        """Route one parsed statement to its analysis path and update ``analysis``.

        ``USE`` statements are intercepted by the caller (``analyze``) before reaching
        here; the check in this method is defensive. Handles ``DROP`` (clears a temp table
        mapping), ``ALTER``/``SET``/``PRAGMA`` (ignored), ``MERGE`` and ``UPDATE``
        (delegated to their dedicated analyzers), and otherwise normalises
        ``CREATE TABLE/VIEW AS``, ``INSERT ... SELECT``/``INSERT OVERWRITE``, plain
        ``SELECT``/set operations, and ``DELETE`` to a target plus a ``SELECT``-like body
        before calling ``_analyze_select``. Plain DDL (a ``CREATE TABLE`` with no
        ``SELECT`` body) registers the target's declared columns directly with no lineage.
        A statement type with no modelled path becomes an ``unsupported_syntax`` item.

        Args:
            stmt: The statement to analyze.
            analysis: Accumulator updated in place with datasets, edges, and inputs/outputs.
            dialect: sqlglot dialect used to render unresolved SQL snippets.
            engine: Executing engine, forwarded to ``_analyze_select``.
            job_id: Recorded on every edge and unresolved item produced.
            default_db: Default database for one-part table names.
            source_file: Recorded on every edge and unresolved item produced.
            target_override: Forced target dataset id, taking precedence over one derived
                from the statement.
        """
        e = stmt.expression
        result = analysis.result
        norm = lambda name: normalize_dataset_id(name, engine=engine, default_db=default_db)  # noqa: E731

        if isinstance(e, exp.Use):
            return
        if isinstance(e, exp.Drop):
            if isinstance(e.this, exp.Table):
                self._temp_tables.pop(norm(_table_name(e.this)), None)
                self._temp_sources.pop(norm(_table_name(e.this)), None)
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
        location: str | None = None
        if isinstance(e, exp.Create):
            table = e.this.this if isinstance(e.this, exp.Schema) else e.this
            if isinstance(table, exp.Table) and e.kind in {"TABLE", "VIEW"}:
                target = target or norm(_table_name(table))
                body = e.expression
                is_temp = any(
                    isinstance(p, exp.TemporaryProperty)
                    for p in (e.args.get("properties") or exp.Properties()).expressions
                ) or bool(e.args.get("temporary"))
                location = _storage_location(e, norm)
                like = e.find(exp.LikeProperty)
                if body is None and like is not None and isinstance(like.this, exp.Table):
                    # ``CREATE TABLE b.t LIKE a.s``: structure copied, a table-level edge.
                    self._table_only(
                        norm(_table_name(like.this)),
                        target,
                        stmt,
                        analysis,
                        Provenance(parser="sqlglot", dialect=dialect),
                        job_id,
                        source_file,
                        e.sql(dialect=dialect),
                    )
                    self._locate(target, location, result)
                    return
                if body is None and isinstance(e.this, exp.Schema):
                    # Plain DDL: register columns, no lineage.
                    cols = [c.name for c in e.this.expressions if isinstance(c, exp.ColumnDef)]
                    if is_temp:
                        self._temp_tables.setdefault(target, {})
                        self._temp_sources.setdefault(target, set())
                        return
                    result.datasets.append(
                        DatasetRef(
                            id=target,
                            namespace=dataset_ref_from_id(target).namespace,
                            name=dataset_ref_from_id(target).name,
                            columns=cols,
                        )
                    )
                    analysis.outputs.add(target)
                    self._locate(target, location, result)
                    return
        elif isinstance(e, exp.Insert):
            table = e.this.this if isinstance(e.this, exp.Schema) else e.this
            if isinstance(table, exp.Table):
                target = target or norm(_table_name(table))
            elif isinstance(table, exp.Directory) and isinstance(table.this, exp.Literal):
                # ``INSERT OVERWRITE DIRECTORY 's3://...'``: the path is the output dataset.
                target = target or norm(table.this.this)
            body = e.expression
            if isinstance(body, exp.Values):
                if target:
                    analysis.outputs.add(target)
                return
        elif isinstance(e, exp.Select) and isinstance(e.args.get("into"), exp.Into):
            # ``SELECT ... INTO b.t FROM ...`` creates ``b.t`` from the projection.
            into = e.args["into"]
            if isinstance(into.this, exp.Table):
                target = target or norm(_table_name(into.this))
            body = e.copy()
            body.set("into", None)
        elif isinstance(e, (exp.Select, exp.SetOperation, exp.Subquery)):
            body = e
        elif isinstance(e, exp.Delete):
            table = e.this
            if isinstance(table, exp.Table):
                ds = norm(_table_name(table))
                analysis.inputs.add(ds)
                analysis.outputs.add(ds)
            # ``DELETE ... WHERE id IN (SELECT ... FROM a.s)`` reads ``a.s``.
            ctes = {c.alias for c in e.find_all(exp.CTE)}
            for t in e.find_all(exp.Table):
                if t is not table and t.name and t.name not in ctes:
                    analysis.inputs.add(norm(_table_name(t)))
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
        if target and not is_temp:
            self._locate(target, location, result)

    def _locate(self, target: str, location: str | None, result: WorkerResult) -> None:
        """Attach a declared storage location to the target dataset as alias evidence.

        Args:
            target: Dataset id the ``CREATE`` statement wrote.
            location: Normalised storage path from ``LOCATION`` or ``external_location``,
                or ``None`` when the statement declared none.
            result: Worker result whose ``datasets`` entry for ``target`` is updated in
                place (``DatasetRef.aliases`` and ``physical_location``).
        """
        if not location:
            return
        for ds in result.datasets:
            if ds.id == target:
                if location not in ds.aliases:
                    ds.aliases.append(location)
                ds.physical_location = ds.physical_location or location

    def _table_only(
        self, source: str, target: str, stmt, analysis, prov, job_id, source_file, expression
    ) -> None:
        """Record a table-level read/write with no column lineage.

        Used for statements whose column mapping is unknown by construction
        (``CREATE TABLE ... LIKE``, ``MERGE ... DELETE``, ``UPDATE SET *`` without a
        schema).

        Args:
            source: Dataset id read.
            target: Dataset id written.
            stmt: Enclosing statement, for line numbers.
            analysis: Accumulator receiving inputs, outputs and the edge.
            prov: Provenance for the edge.
            job_id: Job the edge belongs to.
            source_file: File the statement was read from.
            expression: SQL text recorded on the edge's transformation.
        """
        analysis.inputs.add(source)
        analysis.outputs.add(target)
        analysis.result.table_edges.append(
            TableEdge(
                source=source,
                target=target,
                provenance=prov,
                job_id=job_id,
                source_file=source_file,
                line=stmt.line_start,
                transformation=Transformation(
                    expression=expression,
                    source_file=source_file,
                    line_start=stmt.line_start,
                    line_end=stmt.line_end,
                ),
            )
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
        """Resolve a ``SELECT``-like body's column and table lineage into a target dataset.

        Collects the real (non-CTE) source tables, qualifies the body against the schema
        provider (or an inferred schema), checks for unexpandable ``SELECT *`` (kind
        ``missing_schema``, confidence downgraded to ``partial``), and resolves each output
        projection via ``_column_edge``. When the target column count from an explicit
        column list does not match the number of projections, output columns are dropped
        entirely and an ``unknown_column`` item is recorded. Registers the target dataset
        with its resolved columns, one ``TableEdge`` per source table, and, for a temp
        table, stores its column edges in ``self._temp_tables`` for later expansion.

        Args:
            body: The ``SELECT``/set-operation/subquery body to analyze.
            target: Target dataset id, or ``None`` for a plain ``SELECT`` with no target
                (in which case output columns are recorded under ``"__select__"`` and no
                edges are produced).
            stmt: The enclosing statement, for line numbers.
            analysis: Accumulator updated in place.
            dialect: sqlglot dialect used to qualify and render SQL.
            engine: Executing engine; unused directly here but threaded through for
                consistency with callers that also invoke ``_analyze_merge``.
            job_id: Recorded on every edge and unresolved item produced.
            norm: Callable normalising a raw table name to a canonical dataset id.
            source_file: Recorded on every edge and unresolved item produced.
            is_temp: Whether ``target`` is a session-scoped temp table, so its column
                edges are cached in ``self._temp_tables`` instead of only being reported.
            target_columns: Explicit target column names from ``INSERT``/``CREATE ...
                (cols)``, used to rename and validate output projections.
        """
        result = analysis.result
        prov = Provenance(
            parser="sqlglot", dialect=dialect, confidence="partial" if stmt.holes else "exact"
        )

        # Real tables referenced (CTE names excluded).
        source_tables: dict[str, str] = {}  # sqlglot table sql -> dataset id
        for scope in traverse_scope(body):
            for _, t in scope.selected_sources.values():
                if isinstance(t, exp.Table) and t.name:
                    ds = norm(_table_name(t))
                    source_tables[_table_name(t)] = ds
        # Session temp tables are not datasets: reads route through to their sources.
        is_temp = is_temp or (target in self._temp_tables if target else False)
        physical_sources = self._physical(source_tables.values())
        analysis.inputs.update(physical_sources)
        if target and not is_temp:
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
                            transformation=Transformation(
                                expression=body.sql(dialect=dialect),
                                source_file=source_file,
                                line_start=stmt.line_start,
                                line_end=stmt.line_end,
                            ),
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
        self._emit_join_conditions(indirect["pairs"], prov, job_id, result)
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
            if output_name in out_cols:
                # Two projections with one name (``SELECT x, x`` or a star over a join):
                # keep the first edge and report the ambiguity instead of a duplicate edge.
                result.unresolved.append(
                    Unresolved(
                        kind="unknown_column",
                        source_file=source_file,
                        line=stmt.line_start,
                        reason=f"Duplicate output column name {output_name!r} at projection "
                        f"{position + 1}; only the first projection is tracked",
                        job_id=job_id,
                        expression=proj.sql(dialect=dialect)[:200],
                    )
                )
                continue
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

        if target and is_temp:
            analysis.output_columns[target] = out_cols
            self._temp_tables[target] = {**self._temp_tables.get(target, {}), **edges_for_target}
            self._temp_sources[target] = self._temp_sources.get(target, set()) | physical_sources
        elif target:
            analysis.output_columns[target] = out_cols
            ds_ref = dataset_ref_from_id(target)
            result.datasets.append(ds_ref.model_copy(update={"columns": out_cols}))
            for ds in sorted(physical_sources):
                result.table_edges.append(
                    TableEdge(
                        source=ds,
                        target=target,
                        provenance=prov,
                        job_id=job_id,
                        source_file=source_file,
                        line=stmt.line_start,
                        transformation=Transformation(
                            expression=body.sql(dialect=dialect),
                            source_file=source_file,
                            line_start=stmt.line_start,
                            line_end=stmt.line_end,
                        ),
                    )
                )
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
        """Resolve one output column's ``sqlglot.lineage`` leaves into a ``ColumnEdge``.

        Walks the lineage graph's leaf nodes for ``name``. A leaf backed by a real table
        becomes a source column, checked against the schema provider when available (an
        unknown column downgrades confidence to ``partial`` and is dropped, with an
        ``unknown_column`` item recorded). A leaf with no column references (a constant,
        ``COUNT(*)``, a zero-argument function) contributes no source. Anything else that
        cannot be traced to a table downgrades confidence to ``partial``, except for a
        top-level constant/null/boolean/anonymous-function/current-timestamp/current-date
        projection, which is expected to have no sources.

        Args:
            name: Output column name (alias or bare name) being resolved.
            proj: The projection expression for ``name`` in the qualified query.
            qualified: The fully qualified query passed to ``sqlglot.lineage``.
            schema_map: Nested ``{db: {table: {col: type}}}`` schema used by ``lineage``.
            dialect: sqlglot dialect used to render the transformation expression.
            norm: Callable normalising a raw table name to a canonical dataset id.
            target: Target dataset id the resulting edge's ``ColumnRef`` belongs to.
            job_id: Recorded on the edge and any unresolved item produced.
            indirect: Filter/join/aggregation indirect sources from ``_indirect_sources``,
                attached to the edge regardless of how ``name`` itself was resolved.
            prov: Base provenance to copy onto the edge, with confidence adjusted.
            stmt: The enclosing statement, for line numbers.
            source_file: Recorded on the edge and any unresolved item produced.
            result: Worker result that unresolved items are appended to.

        Returns:
            A ``ColumnEdge`` for ``name``. Despite the ``| None`` annotation, this method
            does not return ``None`` in current code: a ``sqlglot.lineage`` failure still
            yields a ``partial``-confidence edge with no sources.
        """
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
        """Replace sources that point at a session temp table with that table's own sources.

        Applied to every column edge as it is produced, so a temp table created earlier in
        the same ``analyze`` call is transparently expanded to its ultimate table sources
        rather than left pointing at a dataset id that never gets registered as a real
        table.

        Args:
            edge: A freshly resolved column edge whose sources may include temp tables.

        Returns:
            ``edge`` unchanged when there are no known temp tables; otherwise a copy with
            temp-table sources (direct and indirect) replaced by their own upstream
            sources, and confidence downgraded to match the least exact of them.
        """
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
        """Resolve a ``MERGE`` statement's ``WHEN`` branches into column lineage.

        Each ``WHEN MATCHED UPDATE`` or ``WHEN NOT MATCHED INSERT`` branch is rewritten as
        a synthetic ``SELECT`` (assignment expressions aliased to their target column, from
        the merge target joined to the using source on the merge condition, further
        filtered by the branch's own condition) and analyzed with ``_analyze_select``.
        Branches touching the same target column are merged: sources and indirect sources
        are unioned, the reported transformation expression is the original ``MERGE`` text
        (not the synthetic ``SELECT``) so lineage points back at real source SQL, the kind
        becomes ``expression`` when branches disagree, and confidence is downgraded to the
        least exact branch. The merge target is both an input and an output.

        Args:
            e: The parsed ``MERGE`` expression.
            stmt: The enclosing statement, for line numbers.
            analysis: Accumulator updated in place with the merged edges and inputs.
            dialect: sqlglot dialect used to analyze branches and render the merge text.
            job_id: Recorded on every edge and unresolved item produced.
            norm: Callable normalising a raw table name to a canonical dataset id.
            source_file: Recorded on every edge and unresolved item produced.

        Raises:
            KeyError: If a branch is missing the ``"whens"``, ``"using"``, or ``"on"``
                argument sqlglot's ``Merge`` grammar is expected to always provide.
        """
        target = norm(_table_name(e.this))
        analysis.inputs.add(target)
        analysis.outputs.add(target)
        using = e.args.get("using")
        using_alias = using.alias_or_name if isinstance(using, exp.Expression) else None
        merged: dict[str, ColumnEdge] = {}
        star_unresolved = False
        for when in e.args["whens"].expressions:
            action = when.args.get("then")
            pairs = []
            star = (
                isinstance(action, exp.Update)
                and any(isinstance(x, exp.Star) for x in action.expressions)
            ) or (isinstance(action, exp.Insert) and isinstance(action.this, exp.Star))
            if star:
                # ``UPDATE SET *`` / ``INSERT *``: every target column comes from the
                # like-named column of the USING source; needs the target's schema.
                known = self.schema.columns(target) if self.schema else None
                if not known or not using_alias:
                    if star_unresolved:
                        continue
                    star_unresolved = True
                    analysis.result.unresolved.append(
                        Unresolved(
                            kind="missing_schema",
                            source_file=source_file,
                            line=stmt.line_start,
                            reason="MERGE SET */INSERT * needs the target schema to map columns",
                            job_id=job_id,
                        )
                    )
                    continue
                pairs = [(c, exp.column(c, table=using_alias)) for c in known]
            elif isinstance(action, exp.Update):
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
        if not merged and using is not None:
            # ``WHEN MATCHED THEN DELETE`` only, or ``SET *`` without a schema: the USING
            # source still drives the write, so keep it as an input with a table edge and
            # record the ON equalities as join conditions.
            prov = Provenance(
                parser="sqlglot",
                dialect=dialect,
                confidence="partial" if star_unresolved or stmt.holes else "exact",
            )
            ctes = {c.alias for c in e.find_all(exp.CTE)}
            sources = {
                norm(_table_name(t))
                for t in using.find_all(exp.Table)
                if t.name and t.name not in ctes
            }
            for source in sorted(self._physical(sources)):
                self._table_only(
                    source,
                    target,
                    stmt,
                    analysis,
                    prov,
                    job_id,
                    source_file,
                    e.sql(dialect=dialect),
                )
            probe = exp.select(exp.alias_(exp.Literal.number(1), "__merge__", quoted=True))
            probe = probe.from_(e.this.copy()).join(using.copy(), on=e.args["on"].copy())
            if e.args.get("with_"):
                probe.set("with_", e.args["with_"].copy())
            try:
                qualified = qualify(
                    probe,
                    schema=self._schema_map(
                        {_table_name(t): norm(_table_name(t)) for t in probe.find_all(exp.Table)},
                        norm,
                    )
                    or None,
                    dialect=dialect,
                    validate_qualify_columns=False,
                    infer_schema=True,
                    allow_partial_qualification=True,
                )
                pairs = self._indirect_sources(qualified, norm, dialect)["pairs"]
            except SqlglotError:
                pairs = []
            self._emit_join_conditions(pairs, prov, job_id, analysis.result)
        analysis.result.column_edges.extend(merged.values())
        analysis.output_columns[target] = list(merged)

    def _analyze_update(self, e: exp.Update, stmt, analysis, dialect, job_id, norm, source_file):
        """Resolve an ``UPDATE`` statement's ``SET`` assignments into column lineage.

        Rewrites the statement as a synthetic ``SELECT`` (each ``col = expr`` assignment
        aliased to ``col``, from the updated table, cross-joined to an ``UPDATE ... FROM``
        source when present, with the original ``WHERE``/``WITH`` reattached) and analyzes
        it with ``_analyze_select`` against the same table as both source and target.

        Args:
            e: The parsed ``UPDATE`` expression.
            stmt: The enclosing statement, for line numbers.
            analysis: Accumulator updated in place by ``_analyze_select``.
            dialect: sqlglot dialect used to analyze the synthetic select.
            job_id: Recorded on every edge and unresolved item produced.
            norm: Callable normalising a raw table name to a canonical dataset id.
            source_file: Recorded on every edge and unresolved item produced.
        """
        target = norm(_table_name(e.this))
        projections = [
            exp.alias_(eq.expression.copy(), eq.this.name, quoted=True)
            for eq in e.expressions
            if isinstance(eq, exp.EQ) and isinstance(eq.this, exp.Column)
        ]
        query = exp.select(*projections).from_(e.this.copy())
        if e.args.get("from_"):
            source = e.args["from_"].this.copy()
            # sqlglot nests ``UPDATE ... FROM s JOIN u ON ...`` joins under the FROM table;
            # hoist them so the join predicates become indirect sources (finding 25).
            nested = source.args.pop("joins", None) or []
            query = query.join(source, join_type="CROSS")
            for join in nested:
                query.append("joins", join.copy())
        for key in ("where", "with_"):
            if e.args.get(key):
                query.set(key, e.args[key].copy())
        self._analyze_select(
            query, target, stmt, analysis, dialect, "unknown", job_id, norm, source_file, False
        )

    # --------------------------------------------------------------- helpers
    def _emit_join_conditions(self, pairs, prov: Provenance, job_id: str, result: WorkerResult):
        """Record ``JOIN ... ON`` equalities as ``JoinCondition`` observations.

        Each side is expanded through session temp tables (a side that maps to several or
        no physical columns is dropped), the pair is ordered canonically by
        ``(dataset_id, name)`` so ``a.x = b.y`` and ``b.y = a.x`` dedupe, and self-joins
        on the same physical column are skipped. Deduplication and sorting across the
        whole ``analyze`` call happen in ``analyze``.

        Args:
            pairs: ``(left, right)`` ``ColumnRef`` pairs from ``_indirect_sources``.
            prov: Provenance of the enclosing statement (parser, dialect, confidence).
            job_id: Job the observation belongs to.
            result: Worker result the conditions are appended to.
        """
        for left, right in pairs:
            sides = []
            for ref in (left, right):
                upstream = self._temp_tables.get(ref.dataset_id, {}).get(ref.name)
                if upstream is not None:
                    if len(upstream.sources) != 1:
                        break
                    ref = upstream.sources[0]
                elif ref.dataset_id in self._temp_tables:
                    break
                sides.append(ref)
            if len(sides) != 2:
                continue
            a, b = sorted(sides, key=lambda r: (r.dataset_id, r.name))
            if (a.dataset_id, a.name) == (b.dataset_id, b.name):
                continue
            result.join_conditions.append(
                JoinCondition(left=a, right=b, job_id=job_id, provenance=prov)
            )

    def _physical(self, dataset_ids: Iterable[str]) -> set[str]:
        """Replace session temp table ids with the physical datasets they were built from.

        Args:
            dataset_ids: Dataset ids read by a statement, possibly including temp tables
                created earlier in the same ``analyze`` call.

        Returns:
            The set of physical dataset ids: non-temp ids unchanged, temp ids replaced by
            their recorded upstream sources (finding 14: temp tables never reach
            ``inputs``, ``outputs``, ``datasets`` or ``table_edges``).
        """
        out: set[str] = set()
        for ds in dataset_ids:
            if ds in self._temp_tables:
                out |= self._temp_sources.get(ds, set())
            else:
                out.add(ds)
        return out

    def _schema_map(self, source_tables: Mapping[str, str], norm) -> dict:
        """Build the nested ``{db: {table: {col: type}}}`` schema sqlglot expects.

        Columns come from an open temp table first (``self._temp_tables``, whose keys are
        already the temp table's own resolved columns), falling back to ``self.schema``.
        A source table with no known columns is omitted so ``qualify``/``lineage`` treat
        it as schema-less rather than empty.

        Args:
            source_tables: Mapping of the table's SQL text (as it appeared in the query) to
                its normalised dataset id.
            norm: Unused directly here; accepted for a consistent helper signature with
                callers that also normalise table names.

        Returns:
            Nested mapping keyed by each part of the table's SQL name, with a fabricated
            ``"unknown"`` type for every column (sqlglot needs a type per column but this
            worker does not track real types).
        """
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
        """Collect columns referenced only in ``WHERE``, ``JOIN ON``, ``GROUP BY``, or
        ``HAVING``/``QUALIFY`` across every scope of a qualified query.

        For each such clause, walks its columns within the enclosing scope. A column from a
        real table is recorded directly; a column from a subquery/CTE scope is traced with
        a further ``sqlglot.lineage`` call to its leaf table columns. These become the
        indirect sources attached to every output column edge of the query (design section
        8.1 step 5), matching OpenLineage's indirect transformation categories.

        Args:
            qualified: The fully qualified query to scan.
            norm: Callable normalising a raw table name to a canonical dataset id.
            dialect: sqlglot dialect used for the nested ``lineage`` calls.

        Returns:
            Mapping with keys ``"filter"``, ``"join"``, and ``"aggregation"``, each a
            deduplicated list of the ``ColumnRef``s referenced in that clause kind, plus
            ``"pairs"``: the ``(left, right)`` physical column pairs of every ``JOIN ON``
            equality whose two sides each resolve to exactly one physical column (the
            observations behind ``WorkerResult.join_conditions``).
        """
        result: dict[str, list] = {"filter": [], "join": [], "aggregation": [], "pairs": []}
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
                    if isinstance(col, exp.Column):
                        result[kind].extend(self._resolve_column(col, scope, norm, dialect))
                if kind != "join":
                    continue
                for eq in walk_in_scope(clause):
                    if not (
                        isinstance(eq, exp.EQ)
                        and isinstance(eq.this, exp.Column)
                        and isinstance(eq.expression, exp.Column)
                    ):
                        continue
                    left = self._resolve_column(eq.this, scope, norm, dialect)
                    right = self._resolve_column(eq.expression, scope, norm, dialect)
                    if len(left) == 1 and len(right) == 1:
                        result["pairs"].append((left[0], right[0]))
        return {k: (_dedup(v) if k != "pairs" else v) for k, v in result.items()}

    def _resolve_column(self, col: exp.Column, scope: Scope, norm, dialect) -> list[ColumnRef]:
        """Resolve one qualified column reference to the physical table columns behind it.

        Args:
            col: A column node inside ``scope`` whose ``table`` names one of the scope's
                sources.
            scope: The sqlglot scope the column appears in.
            norm: Callable normalising a raw table name to a canonical dataset id.
            dialect: sqlglot dialect used for the nested ``lineage`` call through a
                subquery or CTE source.

        Returns:
            One ``ColumnRef`` when the column belongs to a real table; the leaf table
            columns when it belongs to a subquery/CTE scope; empty when it cannot be tied
            to any source.
        """
        source = scope.sources.get(col.table)
        if isinstance(source, exp.Table):
            return [ColumnRef(dataset_id=norm(_table_name(source)), name=col.name)]
        if not isinstance(source, Scope):
            return []
        node = sqlglot_lineage(
            exp.column(col.name, quoted=True), source.expression, scope=source, dialect=dialect
        )
        return [
            ColumnRef(dataset_id=norm(_table_name(leaf.source)), name=exp.to_column(leaf.name).name)
            for leaf in node.walk()
            if not leaf.downstream and isinstance(leaf.source, exp.Table)
        ]


def _dedup(refs: Iterable[ColumnRef]) -> list[ColumnRef]:
    """Remove duplicate ``ColumnRef``s, keeping first occurrence order.

    Args:
        refs: Column references to deduplicate, compared by ``(dataset_id, name)``.

    Returns:
        The unique references in the order first seen.
    """
    seen = set()
    out = []
    for r in refs:
        if (r.dataset_id, r.name) not in seen:
            seen.add((r.dataset_id, r.name))
            out.append(r)
    return out


def _storage_location(create: exp.Create, norm) -> str | None:
    """Return the normalised storage path declared on a ``CREATE`` statement, if any.

    Recognises Hive/Spark ``LOCATION 's3://...'`` and Trino/Athena
    ``WITH (external_location = 's3://...')``. The path is alias evidence linking the
    catalog table to its physical files (``DatasetRef.aliases``/``physical_location``).

    Args:
        create: The parsed ``CREATE`` expression.
        norm: Callable normalising a raw dataset name or path to a canonical id.

    Returns:
        The normalised path, or ``None`` when no location is declared or it is not a
        string literal.
    """
    for prop in create.find_all(exp.LocationProperty, exp.Property):
        if isinstance(prop, exp.LocationProperty):
            value = prop.this
        elif type(prop) is exp.Property and prop.name.lower() == "external_location":
            value = prop.args.get("value")
        else:
            continue
        if isinstance(value, exp.Literal) and value.is_string and value.this:
            return norm(value.this)
    return None


def _table_name(t: exp.Table) -> str:
    """Render a sqlglot ``Table`` node's parts as a dotted name, e.g. ``db.schema.table``.

    Args:
        t: The table node.

    Returns:
        The dot-joined SQL text of each part, preserving quoting as sqlglot renders it.
    """
    return ".".join(p.sql() for p in t.parts)


def _kind(proj: exp.Expression) -> str:
    """Classify a projection's ``Transformation.kind`` for design section 8.1 step 4.

    Args:
        proj: The (possibly aliased) output projection expression.

    Returns:
        ``"window"`` if the projection contains a window function, ``"aggregation"`` if it
        contains an aggregate function, ``"identity"`` if it is a bare column reference,
        otherwise ``"expression"``.
    """
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
    """Return the first leading ``--`` comment line of a SQL file, used as ``Job.description``.

    Skips header-style comment lines (``Key: value``, matched by whether the first word
    contains a colon) and stops at the first non-comment, non-blank line.

    Args:
        text: Full SQL file text.

    Returns:
        The stripped text of the first plain leading comment line, or ``None`` when there
        is none before the SQL body starts.
    """
    for line in text.splitlines():
        s = line.strip()
        if s.startswith("--"):
            body = s.lstrip("-").strip()
            if body and ":" not in body.split(" ")[0]:
                return body
        elif s:
            break
    return None
