"""Static frame and I/O analysis shared by PySpark, Pandas and Polars.

This module implements PythonWorker (design spec section 8.2) and, by extension,
SparkStaticWorker (section 8.3), which re-exports it directly. Given a ``.py`` file and the
scanned module graph, :class:`PythonWorker` builds the file's import map, matches calls
against the sink table (``etl_parser.scanner.sinks``) to find reads/writes, reconstructs
SQL-carrying string arguments, and hands SQL text to :class:`~etl_parser.workers.sql.SqlWorker`.
It tracks DataFrame-shaped variables (pandas, polars, and PySpark alike) through an AST-driven
interpreter so column-level projections, joins, aggregations, and filters become
:class:`~etl_parser.models.ColumnEdge`/:class:`~etl_parser.models.TableEdge` records, and it
follows one level of calls into helper modules resolved through the module graph.

The interpreter handles a deliberately finite set of AST/frame operations. It never
imports ETL code. Unknown frame operations preserve known sources with partial confidence.
Everything the interpreter cannot resolve is reported as an
:class:`~etl_parser.models.Unresolved` item with a reason, never guessed.
"""

from __future__ import annotations

import ast
import copy
from dataclasses import dataclass, field
from pathlib import Path

from etl_parser.identity import dataset_ref_from_id, normalize_dataset_id
from etl_parser.models import (
    ColumnEdge,
    ColumnRef,
    Job,
    Provenance,
    TableEdge,
    Transformation,
    Unresolved,
    WorkerResult,
)
from etl_parser.scanner.repo import ScanIndex, SourceFile
from etl_parser.scanner.sinks import ENGINE_DIALECT, URL_ENGINE_HINTS, match_sink
from etl_parser.scanner.strings import Folded, fold_string
from etl_parser.workers.base import comment_schedule, first_docstring_line, parse_header
from etl_parser.workers.sql import SqlWorker


@dataclass
class Column:
    """A single tracked DataFrame column's provenance, as understood so far.

    Instances accumulate as the interpreter walks projections, joins, and aggregations;
    :func:`PythonWorker.analyze_source`'s ``write_frame`` turns each into a ``ColumnEdge``.

    Attributes:
        sources (set[tuple[str, str]]): Direct ``(dataset_id, column_name)`` origins that
            feed this column's value.
        text (str): Source text of the expression that produced this column.
        kind (str): Transformation kind, matching ``Transformation.kind`` (for example
            ``"identity"``, ``"expression"``, ``"aggregation"``, ``"window"``).
        partial (bool): Whether this column's provenance is incompletely known, downgrading
            any edge built from it to ``partial`` confidence.
        name (str | None): Output column name, when known (set by ``alias``/``name`` calls
            or by the caller assigning it into a frame).
        indirect (set[tuple[str, str]]): ``(dataset_id, column_name)`` origins referenced
            only indirectly (filter/join/aggregation keys), not in the value itself.
        source_file (str | None): Path of the file the defining expression came from.
        line_start (int | None): First line of the defining expression.
        line_end (int | None): Last line of the defining expression.
    """

    sources: set[tuple[str, str]] = field(default_factory=set)
    text: str = ""
    kind: str = "identity"
    partial: bool = False
    name: str | None = None
    indirect: set[tuple[str, str]] = field(default_factory=set)
    source_file: str | None = None
    line_start: int | None = None
    line_end: int | None = None


@dataclass
class Expression:
    """A deferred, unevaluated AST expression node.

    Wraps ``pyspark.sql.functions``-style values (``F.col``, ``Window.partitionBy``, and
    similar) that are only resolved to a :class:`Column` once applied to a frame.

    Attributes:
        node (ast.AST): The wrapped expression node.
    """

    node: ast.AST


@dataclass
class Frame:
    """A tracked DataFrame variable: its known columns, source datasets, and open questions.

    This is the interpreter's model of a pandas/polars/PySpark DataFrame as it is threaded
    through assignments and chained method calls in :func:`PythonWorker.analyze_source`.

    Attributes:
        columns (dict[str, Column]): Known output columns by name.
        sources (set[str]): Dataset ids this frame was ultimately read from.
        aliases (dict[str, Frame]): Sub-frame aliases introduced by ``.alias(...)``, used to
            resolve dotted column references like ``"left.id"``.
        indirect (set[tuple[str, str]]): ``(dataset_id, column_name)`` origins referenced
            only through filters, join keys, or group-by keys on this frame.
        partial (bool): Whether this frame's shape is only partially known (an unhandled
            operation was applied, or a source could not be resolved).
        open_columns (bool): Whether columns outside ``self.columns`` may still exist (no
            input schema was available to enumerate them), as opposed to ``self.columns``
            being the complete, closed set of columns.
        group (list[str]): Group-by key column names, set by a ``groupBy``/``groupby`` call
            and consumed by the following ``agg``/``sum``/``mean``/etc. call.
        parallel_sources (bool): Whether this frame was read from multiple dataset arguments
            at once (for example ``pd.read_csv([a, b])``), so an unknown column's origin
            fans out across all of ``sources`` rather than just one.
        write_options (dict[str, ast.AST]): Writer builder options accumulated by
            ``.format(...)``/``.option(...)``/``.options(...)`` calls on a ``df.write``
            chain, keyed by option name, with values as their literal AST nodes. A
            ``"path"`` entry supplies the output location for a terminal ``.save()``.
        inferred (bool): Whether this frame's shape was reached through a heuristic (a
            helper function followed one level, or a branch merge), so edges built from it
            are ``inferred`` rather than ``exact`` (design spec section 13).
    """

    columns: dict[str, Column] = field(default_factory=dict)
    sources: set[str] = field(default_factory=set)
    aliases: dict[str, Frame] = field(default_factory=dict)
    indirect: set[tuple[str, str]] = field(default_factory=set)
    partial: bool = False
    open_columns: bool = True
    group: list[str] = field(default_factory=list)
    parallel_sources: bool = False
    write_options: dict[str, ast.AST] = field(default_factory=dict)
    inferred: bool = False

    def column(self, name: str) -> Column:
        """Resolve a column reference against this frame, including dotted alias access.

        Args:
            name (str): Column name to resolve. A dotted prefix matching a key in
                ``self.aliases`` (for example ``"left.id"``) resolves against that aliased
                sub-frame instead.

        Returns:
            Column: A copy of the known column when tracked. When the column is not tracked
            but this frame has exactly one source dataset (or ``parallel_sources`` is set)
            and columns are still open, a best-effort :class:`Column` sourced from every
            source dataset under that name. Otherwise a :class:`Column` marked ``partial``.
        """
        alias, dot, tail = name.partition(".")
        if dot and alias in self.aliases:
            return self.aliases[alias].column(tail)
        if name in self.columns:
            return copy.deepcopy(self.columns[name])
        if self.open_columns and (len(self.sources) == 1 or self.parallel_sources) and not dot:
            return Column({(source, name) for source in self.sources}, name, name=name)
        return Column(text=name, partial=True, name=name)


@dataclass
class Connection:
    """A resolved database connection/engine, tracked so later SQL calls know their dialect.

    Attributes:
        engine (str): Engine name (for example ``"postgres"``, ``"athena"``), derived from a
            connection URL via ``URL_ENGINE_HINTS``.
    """

    engine: str


@dataclass
class Reader:
    """A partially-built PySpark ``spark.read``/``sqlContext.read`` builder chain.

    Accumulates ``.format(...)``/``.option(...)``/``.options(...)`` calls before the chain
    terminates in a load call (``.load()``, ``.parquet()``, ``.table()``, and similar).

    Attributes:
        options (dict[str, ast.AST]): Builder options captured so far, keyed by option name
            (for example ``"path"``, ``"format"``), with values as their literal AST nodes
            (folded to constants once known).
    """

    options: dict[str, ast.AST] = field(default_factory=dict)


class PythonWorker:
    """Static ast-driven lineage worker for Python, pandas, polars, and PySpark files.

    Implements design spec section 8.2 (and, via the ``SparkStaticWorker`` alias, section
    8.3): builds each file's import map, matches calls against the sink table, folds
    SQL/dataset string arguments, tracks DataFrame variables through supported chained
    operations, and follows helper-module calls up to ``max_import_depth`` levels deep.
    Unhandled frame operations degrade the affected columns to ``partial`` confidence and
    continue rather than aborting the analysis.

    Attributes:
        sql (SqlWorker): Worker used to analyze SQL text found in string arguments.
        index (ScanIndex | None): Scanned repository used to resolve local imports and
            helper modules; when ``None``, import following is disabled.
        bindings (dict[str, str]): Explicit values for otherwise-unresolvable symbols or
            environment keys, seeded into every analysis's folding environment.
        default_db (str | None): Default database used to qualify one-part dataset/table
            names.
        max_import_depth (int): Maximum depth of helper-module calls to follow.
    """

    def __init__(
        self,
        sql_worker: SqlWorker | None = None,
        index: ScanIndex | None = None,
        *,
        bindings: dict[str, str] | None = None,
        default_db: str | None = None,
        max_import_depth: int = 3,
    ):
        """Configure a worker for analyzing Python/PySpark files.

        Args:
            sql_worker (SqlWorker | None): Worker to delegate SQL text to; a default
                :class:`~etl_parser.workers.sql.SqlWorker` is created when not given.
            index (ScanIndex | None): Scanned repository, used to resolve imports to helper
                module files. Import following is skipped when ``None``.
            bindings (dict[str, str] | None): Explicit values for otherwise-unresolvable
                symbols or environment keys.
            default_db (str | None): Default database for qualifying one-part table names.
            max_import_depth (int): Maximum depth of helper-module calls to follow before
                recording a ``partial``/``unresolved_import`` result. Defaults to 3.
        """
        self.sql = sql_worker or SqlWorker()
        self.index = index
        self.bindings = bindings or {}
        self.default_db = default_db
        self.max_import_depth = max_import_depth

    def analyze_file(self, path: Path) -> WorkerResult:
        """Read a ``.py`` file from disk and analyze it.

        Args:
            path (Path): Path to the Python file to analyze.

        Returns:
            WorkerResult: The analysis result from :meth:`analyze_source`, or a result
            containing a single ``unsupported_syntax`` item when the file cannot be read.
        """
        root = self.index.root if self.index else path.parent
        try:
            source = SourceFile(path.relative_to(root).as_posix(), path.read_text(), ".py")
        except (OSError, UnicodeError) as exc:
            return WorkerResult(
                unresolved=[
                    Unresolved(kind="unsupported_syntax", source_file=str(path), reason=str(exc))
                ]
            )
        return self.analyze_source(source)

    def analyze_source(
        self,
        source: SourceFile,
        *,
        entry_function: str | None = None,
        job_id_override: str | None = None,
    ) -> WorkerResult:
        """Analyze one Python/PySpark source file and return its lineage contributions.

        This is the core of PythonWorker (design spec section 8.2): it walks the module's
        top-level statements (and, when ``entry_function`` is given, that function's body),
        matching sink calls, tracking DataFrame variables, and following one level of helper
        calls resolved through ``self.index``. When no ``entry_function`` is given, a
        function named ``main``, ``handler``, or ``lambda_handler`` is invoked automatically
        if present and not already reached by another call, so Lambda-style entry points are
        still analyzed as their own job. A :class:`~etl_parser.models.Job` record is emitted
        for the file (or entry function) with its resolved inputs/outputs, owner and
        description from header comments/docstring, and schedule, if any.

        Args:
            source (SourceFile): The file to analyze.
            entry_function (str | None): Name of a specific function to treat as the job's
                entry point, analyzed instead of (in addition to) top-level statements. When
                given, only import/function/assignment statements are executed at module
                level before the function itself is invoked.
            job_id_override (str | None): Job id to use instead of ``source.job_id``.

        Returns:
            WorkerResult: Datasets, the job, column edges, table edges, schedules, and
            unresolved items found while analyzing this file.
        """
        result = WorkerResult()
        inputs: set[str] = set()
        outputs: set[str] = set()
        job_id = job_id_override or source.job_id
        root_source = source
        language = "pyspark" if "pyspark" in source.text else "python"
        default_engine = "spark" if language == "pyspark" else "unknown"
        invoked: set[tuple[str, str]] = set()
        active: set[tuple[str, str]] = set()
        loading_modules: set[str] = set()

        def issue(node, reason, kind="unsupported_syntax", folded=None):
            """Append an :class:`Unresolved` item for the current file at ``node``'s line.

            Args:
                node: AST node the issue is attached to; its source text and line number
                    are recorded.
                reason (str): Human-readable explanation of the issue.
                kind (str): ``Unresolved.kind`` value. Defaults to ``"unsupported_syntax"``.
                folded (Folded | None): The folded value, when the issue came from folding a
                    string expression; supplies ``partial_text``, ``symbols``, and
                    ``assumptions``, and changes the default remediation text.
            """
            result.unresolved.append(
                Unresolved(
                    kind=kind,
                    source_file=state.source.path,
                    line=getattr(node, "lineno", None),
                    reason=reason,
                    job_id=job_id,
                    expression=ast.unparse(node),
                    partial_text=folded.text if folded else None,
                    symbols=folded.placeholders if folded else [],
                    assumptions=folded.assumptions if folded else {},
                    remediation=(
                        "Supply scan bindings, then rescan each environment separately."
                        if folded
                        else "Review this location or add a parser handler."
                    ),
                )
            )

        @dataclass
        class State:
            """Per-module interpreter state: one file's symbol table and import map.

            A new ``State`` is created for the entry module and for each helper module
            loaded via ``imported_state``/``invoke``, so each file's names stay isolated.

            Attributes:
                source (SourceFile): The module this state belongs to.
                env (dict): Symbol table mapping names to their tracked values (``Frame``,
                    ``Column``, ``Folded``, ``Connection``, ``Reader``, nested ``State`` for
                    imported modules, or a plain string/AST value).
                imports (dict): Maps a local import name to ``(dotted_module, level)``.
                functions (dict): Maps a local function name to its ``ast.FunctionDef``.
                depth (int): Helper-module recursion depth, compared against
                    ``self.max_import_depth``.
            """

            source: SourceFile
            env: dict = field(default_factory=lambda: dict(self.bindings))
            imports: dict = field(default_factory=dict)
            functions: dict = field(default_factory=dict)
            depth: int = 0

        state = State(source)

        def strings():
            """Collect string-like environment values for folding, including one import level.

            Returns:
                dict: Every ``str``/``Folded`` value in the current scope's ``env``, plus,
                for each imported module bound to a name, its own string values exposed
                under ``"module.attr"`` keys.
            """
            values = {k: v for k, v in state.env.items() if isinstance(v, (str, Folded))}
            for key, module in state.env.items():
                if isinstance(module, State):
                    values.update(
                        {
                            f"{key}.{k}": v
                            for k, v in module.env.items()
                            if isinstance(v, (str, Folded))
                        }
                    )
            return values

        def folded(node):
            """Fold an AST expression to a :class:`Folded` string using the current scope.

            Args:
                node: Expression node to fold.

            Returns:
                Folded: The folded value, using the current scope's string-like bindings
                (see ``strings``) to resolve names.
            """
            return fold_string(node, strings())

        def concrete(node, kind):
            """Fold ``node`` and require a fully concrete result, else record an issue.

            Args:
                node: Expression node to fold.
                kind (str): ``Unresolved.kind`` to use if the value cannot be resolved.

            Returns:
                str | None: The folded text, or ``None`` (with an issue recorded) when the
                value is incomplete or relies on an environment-default assumption.
            """
            value = folded(node)
            if not value.complete or value.assumptions:
                issue(
                    node, "Reference depends on runtime values or environment defaults", kind, value
                )
                return None
            return value.text

        def callee(node):
            """Render a call target's fully qualified name, resolving local import aliases.

            Args:
                node: The ``Call.func`` expression to render.

            Returns:
                str: The dotted callee text with the leading name resolved through
                ``state.imports`` and, for ``pandas``/``polars``/``awswrangler`` targets,
                rewritten to the ``pd.``/``pl.``/``wr.`` shorthand used by the sink table.
            """
            text = ast.unparse(node)
            first, dot, rest = text.partition(".")
            imported = state.imports.get(first)
            if imported:
                text = imported[0] + (dot + rest if dot else "")
            for package, alias in (("pandas", "pd"), ("polars", "pl"), ("awswrangler", "wr")):
                if text.startswith(package + "."):
                    return alias + text[len(package) :]
            return text

        def col_expr(node, frame):
            """Evaluate a column expression and stamp it with its source location.

            Thin wrapper around ``_col_expr`` that fills in ``source_file``/``line_start``/
            ``line_end`` on the returned :class:`Column` when not already set, so every
            column edge carries the location of the expression that produced it.

            Args:
                node: Expression node to evaluate as a column.
                frame (Frame): Frame the expression is evaluated against.

            Returns:
                Column: The evaluated column, with location fields populated.
            """
            column = _col_expr(node, frame)
            if column.source_file is None:
                column.source_file = state.source.path
                column.line_start = getattr(node, "lineno", None)
                column.line_end = getattr(node, "end_lineno", column.line_start)
            return column

        def _col_expr(node, frame):
            """Evaluate an expression node to a :class:`Column`, tracking provenance.

            Handles names bound to a tracked :class:`Column`/:class:`Expression`, literal
            constants, subscript column access, attribute access on a tracked
            :class:`Frame`, and a broad set of PySpark/pandas/polars column-function calls
            (``col``, ``lit``, ``cast``/``astype``, ``over``, ``alias``/``name``, ``expr``,
            aggregate functions, and a fixed list of scalar column functions). Any other
            node falls back to combining the sources/partial state of its child expression
            nodes. This is the recursive worker behind ``col_expr``, which is called instead
            of it everywhere else in this function.

            Args:
                node: Expression node to evaluate.
                frame (Frame): Frame the expression is evaluated against, used to resolve
                    column references and aggregate group-by keys.

            Returns:
                Column: The evaluated column, without source-location fields set (those are
                filled in by ``col_expr``).
            """
            if isinstance(node, ast.Name) and isinstance(state.env.get(node.id), Column):
                return copy.deepcopy(state.env[node.id])
            if isinstance(node, ast.Name) and isinstance(state.env.get(node.id), Expression):
                return col_expr(state.env[node.id].node, frame)
            if isinstance(node, ast.Name):
                value = state.env.get(node.id)
                return Column(
                    text=node.id,
                    kind="expression",
                    partial=not isinstance(value, Folded) or not value.complete,
                )
            if isinstance(node, ast.Constant):
                return Column(text=repr(node.value), kind="expression")
            if isinstance(node, ast.Subscript):
                owner = evaluate(node.value)
                key = folded(node.slice)
                if isinstance(owner, Frame) and key.complete:
                    return owner.column(key.text)
            if isinstance(node, ast.Attribute):
                owner = state.env.get(node.value.id) if isinstance(node.value, ast.Name) else None
                if isinstance(owner, Frame):
                    return owner.column(node.attr)
            if isinstance(node, ast.Call):
                method = (
                    node.func.attr
                    if isinstance(node.func, ast.Attribute)
                    else ast.unparse(node.func)
                )
                if method in {"col", "column"} and node.args:
                    name = folded(node.args[0])
                    return frame.column(name.text) if name.complete else Column(partial=True)
                if method in {"lit", "literal"}:
                    return Column(text=ast.unparse(node), kind="expression")
                if method in {"cast", "astype"}:
                    value = col_expr(node.func.value, frame)
                    value.text = ast.unparse(node)
                    return value
                if method == "over":
                    value = col_expr(node.func.value, frame)
                    value.kind, value.text = "window", ast.unparse(node)
                    for arg in node.args:
                        window = col_expr(arg, frame)
                        value.indirect |= window.sources | window.indirect
                        value.partial |= window.partial
                    return value
                if method in {"alias", "name"} and isinstance(node.func, ast.Attribute):
                    value = col_expr(node.func.value, frame)
                    value.name = folded(node.args[0]).text if node.args else value.name
                    return value
                if method in {"expr"} and node.args:
                    return sql_expression(node.args[0], frame)
                aggregate = method in {
                    "sum",
                    "avg",
                    "mean",
                    "min",
                    "max",
                    "count",
                    "countDistinct",
                    "n_unique",
                    "stddev",
                    "std",
                    "collect_list",
                    "first",
                    "last",
                }
                values = []
                if isinstance(node.func, ast.Attribute):
                    base = node.func.value
                    if isinstance(base, (ast.Call, ast.Subscript, ast.BinOp, ast.UnaryOp)) or (
                        isinstance(base, ast.Name) and isinstance(state.env.get(base.id), Column)
                    ):
                        values.append(col_expr(base, frame))
                column_functions = {
                    "unix_timestamp",
                    "months_between",
                    "trunc",
                    "add_months",
                    "orderBy",
                    "partitionBy",
                    "lag",
                    "lead",
                    "upper",
                    "lower",
                    "trim",
                    "ltrim",
                    "rtrim",
                    "length",
                    "substring",
                    "substr",
                    "regexp_replace",
                    "regexp_extract",
                    "to_date",
                    "to_timestamp",
                    "date_format",
                    "year",
                    "month",
                    "dayofmonth",
                    "dayofweek",
                    "weekofyear",
                    "datediff",
                    "date_add",
                    "date_sub",
                    "quarter",
                    "round",
                    "abs",
                    "floor",
                    "ceil",
                    "sha2",
                    "md5",
                    "split",
                    "coalesce",
                    "concat",
                    "concat_ws",
                    "greatest",
                    "least",
                }
                for position, argument in enumerate(node.args):
                    if (
                        (
                            aggregate
                            or method in column_functions
                            and (
                                position == 0
                                or method
                                in {
                                    "coalesce",
                                    "concat",
                                    "greatest",
                                    "least",
                                    "orderBy",
                                    "partitionBy",
                                }
                                or method == "concat_ws"
                                and position > 0
                            )
                        )
                        and not (method == "concat_ws" and position == 0)
                        and isinstance(argument, ast.Constant)
                        and isinstance(argument.value, str)
                    ):
                        if argument.value != "*":
                            values.append(frame.column(argument.value))
                    else:
                        values.append(col_expr(argument, frame))
                known = (
                    aggregate
                    or method in column_functions
                    or method
                    in {
                        "between",
                        "startswith",
                        "endswith",
                        "contains",
                        "desc",
                        "asc",
                        "explode",
                        "sequence",
                        "rowsBetween",
                        "rangeBetween",
                        "cast",
                        "astype",
                        "when",
                        "otherwise",
                        "coalesce",
                        "concat",
                        "concat_ws",
                        "upper",
                        "lower",
                        "trim",
                        "ltrim",
                        "rtrim",
                        "length",
                        "substring",
                        "substr",
                        "regexp_replace",
                        "regexp_extract",
                        "to_date",
                        "to_timestamp",
                        "date_format",
                        "year",
                        "month",
                        "dayofmonth",
                        "datediff",
                        "date_add",
                        "date_sub",
                        "quarter",
                        "isNull",
                        "isNotNull",
                        "is_null",
                        "is_not_null",
                        "isin",
                        "is_in",
                        "fill_null",
                        "round",
                        "abs",
                        "floor",
                        "ceil",
                        "current_timestamp",
                        "current_date",
                        "over",
                        "row_number",
                        "rank",
                        "dense_rank",
                        "lag",
                        "lead",
                        "monotonically_increasing_id",
                        "sha2",
                        "md5",
                        "split",
                        "getItem",
                        "greatest",
                        "least",
                        "nullif",
                        "fillna",
                    }
                )
                return Column(
                    set().union(*(v.sources for v in values)),
                    ast.unparse(node),
                    "window" if method == "over" else "aggregation" if aggregate else "expression",
                    not known or any(v.partial for v in values),
                    next((v.name for v in values if v.name), None),
                    set().union(*(v.indirect for v in values)),
                )
            children = [
                col_expr(n, frame) for n in ast.iter_child_nodes(node) if isinstance(n, ast.expr)
            ]
            return Column(
                set().union(*(v.sources for v in children)),
                ast.unparse(node),
                "expression",
                any(v.partial for v in children),
                indirect=set().union(*(v.indirect for v in children)),
            )

        def sql_expression(node, frame):
            """Evaluate a raw SQL expression string (``F.expr``/``selectExpr``) via sqlglot.

            Qualifies the text against a synthetic single-column-list relation built from
            ``frame``'s known columns, then rewrites the resulting column edge's sources
            back onto ``frame``'s actual column origins.

            Args:
                node: Expression node holding the SQL text (folded via ``concrete``).
                frame (Frame): Frame whose columns the SQL expression is evaluated against.

            Returns:
                Column: The evaluated column. Marked ``partial`` when the text cannot be
                folded to a concrete string or sqlglot produces no column edge for it.
            """
            text = concrete(node, "dynamic_sql")
            if text is None:
                return Column(partial=True)
            # Qualify against a synthetic relation, then replace its columns with frame origins.
            from etl_parser.workers.sql import DictSchemaProvider

            worker = SqlWorker(DictSchemaProvider({"frame": {"input": list(frame.columns)}}))
            parsed = worker.analyze(
                f"SELECT {text} AS value FROM frame.input",
                dialect="spark",
                target_override="frame://expression/output",
                job_id=job_id,
            )
            if not parsed.result.column_edges:
                return Column(text=text, partial=True)
            edge = parsed.result.column_edges[0]
            refs = [frame.column(c.name) for c in edge.sources]
            return Column(
                set().union(*(c.sources for c in refs)),
                text,
                edge.transformation.kind,
                bool(parsed.result.unresolved) or any(c.partial for c in refs),
            )

        def select_frame(frame, args):
            """Build a new frame containing only the selected columns/expressions.

            Backs ``select``/``selectExpr``-style calls. Flattens list/tuple arguments,
            expands a bare ``"*"`` string to all currently-known columns, and evaluates
            non-string arguments as column expressions via ``col_expr``.

            Args:
                frame (Frame): Source frame the selection is applied to.
                args: Sequence of AST argument nodes naming or computing output columns.

            Returns:
                Frame: A copy of ``frame`` with ``columns`` replaced by the selection and
                ``open_columns`` set to ``False`` (except where ``"*"`` preserves it),
                marked ``partial`` for any argument whose output column name could not be
                determined.
            """
            out = copy.deepcopy(frame)
            out.columns = {}
            out.open_columns = False
            flattened = []
            for arg in args:
                flattened.extend(arg.elts if isinstance(arg, (ast.List, ast.Tuple)) else [arg])
            for arg in flattened:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    name = arg.value
                    if name == "*":
                        out.columns.update(copy.deepcopy(frame.columns))
                        out.open_columns = frame.open_columns
                        continue
                    value = frame.column(name)
                    name = name.rsplit(".", 1)[-1]
                else:
                    value = col_expr(arg, frame)
                    name = value.name
                if name:
                    out.columns[name] = value
                else:
                    out.partial = True
                    issue(arg, "Cannot determine output column name", "unknown_column")
            return out

        def frame_from_dataset(ds):
            """Create a :class:`Frame` for a dataset read, recording it as a job input.

            Args:
                ds (str): Normalized dataset id being read.

            Returns:
                Frame: A frame sourced from ``ds``, with columns seeded from the schema
                provider when available; ``open_columns`` is ``True`` when no schema was
                found, since unlisted columns may still exist.
            """
            inputs.add(ds)
            columns = self.sql.schema.columns(ds) if self.sql.schema else None
            return Frame(
                {c: Column({(ds, c)}, c, name=c) for c in columns or []},
                {ds},
                open_columns=columns is None,
            )

        def write_frame(frame, ds, node):
            """Record a dataset write: table edges from every known source, column edges.

            Emits one ``TableEdge`` per source dataset ``frame`` was built from (or, when
            ``frame`` is not a tracked :class:`Frame`, per input seen so far in this file,
            conservatively) and, when ``frame`` is a tracked :class:`Frame`, one
            ``ColumnEdge`` per known column. Records issues for an untracked write receiver,
            an output with open (possibly incomplete) columns, and any column whose
            provenance is only partially known.

            Args:
                frame (Frame): The frame being written, or any other tracked value when the
                    write target could not be resolved to a frame.
                ds (str): Normalized dataset id being written.
                node: The write call node, used for its source text and line range.
            """
            outputs.add(ds)
            refs = sorted(frame.sources) if isinstance(frame, Frame) else sorted(inputs)
            partial = not isinstance(frame, Frame) or frame.partial
            for src in refs:
                result.table_edges.append(
                    TableEdge(
                        source=src,
                        target=ds,
                        job_id=job_id,
                        source_file=state.source.path,
                        line=node.lineno,
                        transformation=Transformation(
                            expression=ast.unparse(node),
                            source_file=state.source.path,
                            line_start=node.lineno,
                            line_end=getattr(node, "end_lineno", node.lineno),
                        ),
                        provenance=Provenance(
                            parser="python_ast", confidence="partial" if partial else "exact"
                        ),
                    )
                )
            if not isinstance(frame, Frame):
                issue(
                    node,
                    "Write receiver cannot be tracked; table inputs are conservative",
                    "unknown_column",
                )
                return
            if frame.open_columns:
                issue(node, "Output contains columns requiring an input schema", "missing_schema")
            for name, column in frame.columns.items():
                confidence = "partial" if partial or column.partial else "exact"
                if column.partial:
                    issue(
                        node,
                        f"Output {name!r} contains unresolved columns or unsupported expressions",
                        "unknown_column",
                    )
                result.column_edges.append(
                    ColumnEdge(
                        target=ColumnRef(dataset_id=ds, name=name),
                        sources=[
                            ColumnRef(dataset_id=d, name=c) for d, c in sorted(column.sources)
                        ],
                        indirect_sources=[
                            ColumnRef(dataset_id=d, name=c)
                            for d, c in sorted(frame.indirect | column.indirect)
                        ],
                        transformation=Transformation(
                            expression=column.text,
                            kind=column.kind,
                            source_file=column.source_file or state.source.path,
                            line_start=column.line_start or node.lineno,
                            line_end=column.line_end or getattr(node, "end_lineno", node.lineno),
                        ),
                        provenance=Provenance(
                            parser="spark_static" if language == "pyspark" else "pandas_chain",
                            confidence=confidence,
                        ),
                        job_id=job_id,
                    )
                )

        def invoke(function, args, keywords, module_state):
            """Bind arguments and run a function body under its own interpreter state.

            Guards against unbounded recursion/import depth via ``active`` and
            ``max_import_depth``, binds positional and keyword arguments (falling back to
            evaluated defaults, or an unknown placeholder when neither is supplied) into a
            copy of ``module_state``'s environment, then interprets the function body.

            Args:
                function (ast.FunctionDef): The function to invoke.
                args (list): Already-evaluated positional argument values.
                keywords (dict): Already-evaluated keyword argument values by name.
                module_state (State): The state of the module ``function`` is defined in,
                    used as the base environment for the call.

            Returns:
                The function's ``return`` value, as produced by ``statements``, or ``None``
                when the recursion/depth limit is reached or the function has no return.
            """
            nonlocal state
            key = (module_state.source.path, function.name)
            if key in active or module_state.depth > self.max_import_depth:
                issue(function, "Helper recursion/import depth limit reached", "unresolved_import")
                return None
            old = state
            state = copy.copy(module_state)
            state.env = dict(module_state.env)
            parameters = [*function.args.posonlyargs, *function.args.args]
            for p, value in zip(parameters, args, strict=False):
                state.env[p.arg] = value
            for p in parameters[len(args) :]:
                state.env[p.arg] = keywords.get(p.arg, Folded("{{?}}", False, [p.arg]))
            for p, default in zip(
                parameters[-len(function.args.defaults) :], function.args.defaults, strict=False
            ):
                if p.arg not in keywords and parameters.index(p) >= len(args):
                    state.env[p.arg] = evaluate(default)
            active.add(key)
            invoked.add(key)
            try:
                return statements(function.body)
            finally:
                active.remove(key)
                state = old

        def imported_state(module, level=0):
            """Resolve and load a helper module's definitions (functions and constants).

            Loads only import/function/assignment statements (``definitions_only=True``),
            so importing a helper module never executes its top-level side effects, and
            guards against re-entering a module already being loaded or exceeding
            ``max_import_depth``.

            Args:
                module (str): Dotted module name to resolve.
                level (int): Relative import level; ``0`` for an absolute import.

            Returns:
                State | None: The loaded module's state, or ``None`` when ``self.index`` is
                unset, the module cannot be resolved, it is already being loaded (import
                cycle), or the depth limit is reached.
            """
            if not self.index:
                return None
            helper = self.index.resolve_module(module, state.source, level)
            if helper is None:
                return None
            if helper.path in loading_modules or state.depth >= self.max_import_depth:
                return None
            module_state = State(helper, depth=state.depth + 1)
            loading_modules.add(helper.path)
            try:
                load_module(helper, module_state, definitions_only=True)
            finally:
                loading_modules.remove(helper.path)
            return module_state

        def evaluate(node):
            """Evaluate an arbitrary AST expression to its tracked runtime-shaped value.

            This is the interpreter's dispatch core. Depending on ``node`` it returns: the
            bound value or a folded string for a ``Name``; a :class:`Folded` constant; a
            folded string, or a column-tracking :class:`Column` for arithmetic/f-strings
            that touch frame columns; a :class:`Frame`/:class:`Column`/folded value for
            subscript access (list/tuple selection, string column access, group-by-aware
            single-key access, or an indexed slice); a :class:`Reader` builder, a resolved
            attribute, or a folded value for attribute access; and, for calls, one of many
            outcomes depending on the callee: an invoked helper function's return value; a
            :class:`Connection` for ``create_engine``/``connect``; an updated
            :class:`Reader` for builder calls (``format``/``option``/``options``); a
            :class:`Column` for calls on a tracked column; an updated :class:`Frame` for
            supported chained DataFrame operations (``select``, ``withColumn``, ``rename``,
            ``drop``, ``filter``/``where``, ``join``/``merge``, ``groupBy``/``agg``,
            ``union``, and several no-op passthroughs), with unsupported operations
            downgrading the frame to ``partial`` and recording an issue; for a call matching
            the sink table, a new input :class:`Frame`, the write receiver echoed back after
            recording a write, or ``None`` after a SQL write; an :class:`Expression` for
            unresolved ``pyspark.sql.functions``/``Window``-style calls; or a folded value
            as the fallback.

            Args:
                node: Expression node to evaluate, or ``None``.

            Returns:
                The tracked value described above, or ``None`` when ``node`` is ``None`` or
                the call could not be resolved to a usable value (dataset argument missing,
                SQL write with no output frame requested, and similar cases handled by
                returning ``None``).
            """
            if node is None:
                return None
            if isinstance(node, ast.Name):
                return state.env.get(node.id, folded(node))
            if isinstance(node, ast.Constant):
                return fold_string(node)
            if isinstance(node, (ast.JoinedStr, ast.BinOp)):
                f = folded(node)
                if f.complete or isinstance(node, ast.JoinedStr):
                    return f
                # Series arithmetic must preserve frame-column sources.
                return col_expr(node, Frame())
            if isinstance(node, ast.Subscript):
                base = evaluate(node.value)
                if isinstance(base, Frame):
                    if isinstance(node.slice, (ast.List, ast.Tuple)):
                        return select_frame(base, node.slice.elts)
                    if isinstance(node.slice, ast.Constant) and isinstance(node.slice.value, str):
                        if base.group:
                            return select_frame(
                                base, [node.slice, *[ast.Constant(k) for k in base.group]]
                            )
                        return base.column(node.slice.value)
                    out = copy.deepcopy(base)
                    out.indirect |= col_expr(node.slice, base).sources
                    return out
                return folded(node)
            if isinstance(node, ast.Attribute):
                if node.attr == "read" and ast.unparse(node.value) in {"spark", "sqlContext"}:
                    return Reader()
                base = evaluate(node.value)
                if isinstance(base, State):
                    return base.env.get(node.attr, folded(node))
                return base if node.attr in {"write", "read", "loc", "iloc"} else folded(node)
            if not isinstance(node, ast.Call):
                return folded(node)
            name = callee(node.func)
            method = (
                node.func.attr if isinstance(node.func, ast.Attribute) else name.rsplit(".", 1)[-1]
            )
            keywords = {kw.arg: kw.value for kw in node.keywords if kw.arg}
            # Resolve repository helpers before generic I/O suffixes such as load/save.
            if isinstance(node.func, ast.Name) and node.func.id in state.functions:
                return invoke(
                    state.functions[node.func.id],
                    [evaluate(a) for a in node.args],
                    {k: evaluate(v) for k, v in keywords.items()},
                    state,
                )
            first = ast.unparse(node.func).split(".")[0]
            imported = state.imports.get(first)
            if imported and self.index:
                module_name, level = imported
                lookup = (
                    module_name
                    if isinstance(node.func, ast.Attribute)
                    else module_name.rsplit(".", 1)[0]
                )
                helper_state = imported_state(lookup, level)
                if helper_state:
                    function = helper_state.functions.get(method)
                    if function:
                        return invoke(
                            function,
                            [evaluate(a) for a in node.args],
                            {k: evaluate(v) for k, v in keywords.items()},
                            helper_state,
                        )
                    issue(
                        node,
                        f"Helper {method!r} not found in {helper_state.source.path}",
                        "unresolved_import",
                    )
                    return None
                if lookup in self.index.module_map:
                    issue(
                        node,
                        f"Helper import is ambiguous or exceeds depth limit: {lookup}",
                        "unresolved_import",
                    )
                    return None
            if method in {"create_engine", "connect"} and node.args:
                url = folded(node.args[0]).text
                engine = next(
                    (value for key, value in URL_ENGINE_HINTS.items() if key in url), "unknown"
                )
                return Connection(engine)
            receiver = evaluate(node.func.value) if isinstance(node.func, ast.Attribute) else None
            if isinstance(receiver, Reader) and method in {"format", "option", "options"}:
                reader = copy.deepcopy(receiver)
                if method == "option" and len(node.args) >= 2:
                    reader.options[folded(node.args[0]).text] = node.args[1]
                elif method == "options":
                    reader.options.update(keywords)
                elif method == "format" and node.args:
                    reader.options["format"] = node.args[0]
                # Capture values at the builder call rather than re-read reassigned variables.
                for key, value in list(reader.options.items()):
                    resolved = concrete(value, "dynamic_path")
                    if resolved is not None:
                        reader.options[key] = ast.Constant(resolved)
                return reader
            if isinstance(receiver, Column):
                return col_expr(node, Frame())
            if isinstance(receiver, Frame) and method not in {
                "saveAsTable",
                "insertInto",
                "parquet",
                "csv",
                "json",
                "orc",
                "save",
                "to_sql",
                "to_parquet",
                "to_csv",
                "to_json",
                "write_parquet",
                "write_csv",
                "sink_parquet",
            }:
                out = copy.deepcopy(receiver)
                if method in {"alias"}:
                    out.aliases[folded(node.args[0]).text] = receiver
                elif method in {"select", "selectExpr"}:
                    if method == "selectExpr":
                        out.columns = {}
                        out.open_columns = False
                        for arg in node.args:
                            text = folded(arg).text
                            import sqlglot

                            expr = sqlglot.parse_one(text, read="spark")
                            value = sql_expression(ast.Constant(expr.unalias().sql()), receiver)
                            out.columns[expr.alias_or_name] = value
                    else:
                        out = select_frame(receiver, node.args)
                elif method in {"withColumn", "with_columns", "assign"}:
                    if method == "withColumn":
                        out.columns[folded(node.args[0]).text] = col_expr(node.args[1], receiver)
                    elif method == "assign":
                        for key, value in keywords.items():
                            if isinstance(value, ast.Lambda):
                                old = dict(state.env)
                                state.env[value.args.args[0].arg] = receiver
                                out.columns[key] = col_expr(value.body, receiver)
                                state.env = old
                            else:
                                out.columns[key] = col_expr(value, receiver)
                    else:
                        selected = select_frame(receiver, node.args)
                        out.columns.update(selected.columns)
                        out.partial |= selected.partial
                        for key, value in keywords.items():
                            out.columns[key] = col_expr(value, receiver)
                elif method == "toDF":
                    names = [folded(a) for a in node.args]
                    if (
                        receiver.open_columns
                        or len(names) != len(receiver.columns)
                        or any(not n.complete for n in names)
                    ):
                        out.partial = True
                        issue(
                            node,
                            "toDF requires known input columns and matching literal names",
                            "unknown_column",
                        )
                    else:
                        out.columns = {
                            name.text: column
                            for name, column in zip(names, out.columns.values(), strict=True)
                        }
                elif method in {"withColumnRenamed", "rename"}:
                    mapping = {}
                    if method == "withColumnRenamed":
                        mapping = {folded(node.args[0]).text: folded(node.args[1]).text}
                    else:
                        arg = keywords.get("columns") or (node.args[0] if node.args else None)
                        if isinstance(arg, ast.Dict):
                            mapping = {
                                folded(k).text: folded(v).text
                                for k, v in zip(arg.keys, arg.values, strict=True)
                            }
                    for old, new in mapping.items():
                        out.columns[new] = receiver.column(old)
                        out.columns.pop(old, None)
                elif method in {"drop"}:
                    args = list(node.args)
                    if "columns" in keywords:
                        args.append(keywords["columns"])
                    for arg in args:
                        for item in arg.elts if isinstance(arg, (ast.List, ast.Tuple)) else [arg]:
                            out.columns.pop(folded(item).text, None)
                elif method in {"filter", "where", "query"}:
                    for arg in node.args:
                        value = (
                            sql_expression(arg, receiver)
                            if isinstance(arg, ast.Constant) and isinstance(arg.value, str)
                            else col_expr(arg, receiver)
                        )
                        out.indirect |= value.sources
                        out.partial |= value.partial
                elif method in {"join", "merge"}:
                    right = evaluate(node.args[0] if node.args else keywords.get("right"))
                    if isinstance(right, Frame):
                        out.parallel_sources = False
                        out.sources |= right.sources
                        out.aliases.update(right.aliases)
                        out.indirect |= right.indirect
                        out.partial |= right.partial
                        out.open_columns |= right.open_columns
                        for key, value in right.columns.items():
                            if key not in out.columns:
                                out.columns[key] = value
                            elif out.columns[key].sources != value.sources:
                                out.columns[key] = Column(text=key, partial=True, name=key)
                        on = keywords.get("on") or (node.args[1] if len(node.args) > 1 else None)
                        keys = (
                            on.elts if isinstance(on, (ast.List, ast.Tuple)) else [on] if on else []
                        )
                        for key in keys:
                            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                                left_col, right_col = (
                                    receiver.column(key.value),
                                    right.column(key.value),
                                )
                                out.indirect |= left_col.sources | right_col.sources
                                out.columns[key.value] = left_col
                            else:
                                value = col_expr(key, out)
                                out.indirect |= value.sources
                                out.partial |= value.partial
                        for key, frame in (("left_on", receiver), ("right_on", right)):
                            if key in keywords:
                                arg = keywords[key]
                                for item in (
                                    arg.elts if isinstance(arg, (ast.List, ast.Tuple)) else [arg]
                                ):
                                    out.indirect |= frame.column(folded(item).text).sources
                        if method == "merge":
                            suffixes = keywords.get("suffixes")
                            suffixes = (
                                [folded(a).text for a in suffixes.elts]
                                if isinstance(suffixes, (ast.List, ast.Tuple))
                                else ["_x", "_y"]
                            )
                            join_names = {k.value for k in keys if isinstance(k, ast.Constant)}
                            if on is None and not {"left_on", "right_on"} & keywords.keys():
                                join_names = receiver.columns.keys() & right.columns.keys()
                                for key in join_names:
                                    out.indirect |= receiver.column(key).sources
                                    out.indirect |= right.column(key).sources
                                    out.columns[key] = receiver.column(key)
                            if len(suffixes) == 2:
                                overlap = receiver.columns.keys() & right.columns.keys()
                                for key in overlap - join_names:
                                    out.columns.pop(key, None)
                                    out.columns[key + suffixes[0]] = receiver.column(key)
                                    out.columns[key + suffixes[1]] = right.column(key)
                    else:
                        out.partial = True
                        issue(node, "Join input could not be resolved", "unknown_column")
                elif method in {"groupBy", "groupby", "group_by"}:
                    args = (
                        node.args[0].elts
                        if node.args and isinstance(node.args[0], (ast.List, ast.Tuple))
                        else node.args
                    )
                    out.group = [folded(a).text for a in args]
                    for key in out.group:
                        out.indirect |= receiver.column(key).sources
                elif method in {"agg", "aggregate"}:
                    out = select_frame(receiver, node.args)
                    for key in receiver.group:
                        out.columns[key] = receiver.column(key)
                    for key, value in keywords.items():
                        if isinstance(value, ast.Tuple) and len(value.elts) == 2:
                            col = receiver.column(folded(value.elts[0]).text)
                            col.kind, col.text = "aggregation", ast.unparse(value)
                            out.columns[key] = col
                elif method in {"sum", "mean", "min", "max", "count"}:
                    if receiver.group:
                        for key, col in out.columns.items():
                            if key not in receiver.group:
                                col.kind = "aggregation"
                                col.text = f"{method}({col.text})"
                        out.open_columns = False
                    else:
                        return Column(text=ast.unparse(node), kind="aggregation")
                elif method in {"union", "unionByName", "vstack"}:
                    right = evaluate(node.args[0])
                    if isinstance(right, Frame):
                        out.sources |= right.sources
                        out.indirect |= right.indirect
                        out.partial |= right.partial
                        out.open_columns |= right.open_columns
                        for position, key in enumerate(list(out.columns)):
                            right_key = (
                                key
                                if method != "union"
                                else (
                                    list(right.columns)[position]
                                    if position < len(right.columns)
                                    else ""
                                )
                            )
                            col = right.column(right_key)
                            out.columns[key].sources |= col.sources
                            out.columns[key].partial |= col.partial
                        if method == "unionByName":
                            for key in right.columns.keys() - out.columns.keys():
                                out.columns[key] = right.columns[key]
                    else:
                        out.partial = True
                elif method in {"format", "option", "options"}:
                    # Writer builder options; ".option('path', ...)" names the output.
                    if method == "option" and len(node.args) >= 2:
                        out.write_options[folded(node.args[0]).text] = node.args[1]
                    elif method == "options":
                        out.write_options.update(keywords)
                    elif node.args:
                        out.write_options["format"] = node.args[0]
                    # Capture the path at the builder call, not after a later reassignment.
                    path = out.write_options.get("path")
                    if path is not None and not isinstance(path, ast.Constant):
                        resolved = concrete(path, "dynamic_path")
                        if resolved is not None:
                            out.write_options["path"] = ast.Constant(resolved)
                elif method in {
                    "mode",
                    "partitionBy",
                    "copy",
                    "dropDuplicates",
                    "drop_duplicates",
                    "distinct",
                    "unique",
                    "orderBy",
                    "sort",
                    "sort_values",
                    "sort_index",
                    "reset_index",
                    "repartition",
                    "coalesce",
                    "cache",
                    "persist",
                    "collect",
                    "lazy",
                }:
                    pass
                else:
                    out.partial = True
                    issue(node, f"Frame method {method!r} has no handler", "unknown_column")
                return out

            sink = match_sink(name)
            if isinstance(receiver, Reader) and method in {
                "load",
                "parquet",
                "csv",
                "json",
                "orc",
                "text",
                "table",
            }:
                sink = match_sink("read." + method)
            if isinstance(receiver, Frame) and method in {
                "parquet",
                "csv",
                "json",
                "orc",
                "text",
                "save",
            }:
                # Writer builder chains (df.write.mode(...).parquet(path)) lose the
                # "write." prefix from the callee text, so resolve them the way readers are.
                writer = match_sink("write." + method)
                sink = writer if writer and writer.direction == "write" else sink
            if method in {"load", "save"} and not isinstance(receiver, (Reader, Frame)):
                sink = None
            if sink:
                arg = (
                    node.args[sink.arg]
                    if isinstance(sink.arg, int) and sink.arg < len(node.args)
                    else keywords.get(sink.arg if isinstance(sink.arg, str) else sink.alt_arg)
                )
                if arg is None and isinstance(receiver, Reader):
                    arg = receiver.options.get("path")
                if arg is None and isinstance(receiver, Frame):
                    arg = receiver.write_options.get("path")
                if arg is None:
                    # Builder calls such as .load() require option tracking, not a guessed path.
                    issue(node, "Dataset argument is unavailable", "dynamic_table_name")
                    return None
                kind = (
                    "dynamic_sql"
                    if sink.direction == "sql"
                    else "dynamic_path"
                    if sink.scheme != "table"
                    else "dynamic_table_name"
                )
                path_args = (
                    (
                        list(arg.elts)
                        if isinstance(arg, (ast.List, ast.Tuple))
                        else (list(node.args) or [arg])
                        if isinstance(receiver, Reader) and method == "parquet"
                        else [arg]
                    )
                    if sink.direction == "read" and sink.scheme == "path"
                    else [arg]
                )
                value = concrete(path_args[0], kind) if path_args else None
                if value is None:
                    return None
                engine = sink.engine
                if engine in {"unknown", "pandas", "polars"}:
                    connection = (
                        evaluate(node.args[1])
                        if len(node.args) > 1
                        else evaluate(keywords.get("con") or keywords.get("connection"))
                    )
                    engine = (
                        connection.engine if isinstance(connection, Connection) else default_engine
                    )
                if sink.direction == "sql":
                    dialect = sink.dialect or ENGINE_DIALECT.get(engine, "")
                    if not dialect:
                        issue(node, "SQL dialect cannot be determined from the call/connection")
                    target = f"frame://{job_id}/{node.lineno}"
                    analyzed = self.sql.analyze(
                        value,
                        dialect=dialect,
                        engine=engine,
                        default_db=self.default_db,
                        source_file=state.source.path,
                        line_offset=node.lineno - 1,
                        job_id=job_id,
                        target_override=None,
                    )
                    inputs.update(analyzed.inputs)
                    outputs.update(analyzed.outputs)
                    result.extend(analyzed.result)
                    if analyzed.outputs:
                        return None
                    frame_analysis = self.sql.analyze(
                        value,
                        dialect=dialect,
                        engine=engine,
                        default_db=self.default_db,
                        source_file=state.source.path,
                        line_offset=node.lineno - 1,
                        job_id=job_id,
                        target_override=target,
                    )
                    frame = Frame(sources=set(analyzed.inputs), open_columns=False)
                    for edge in frame_analysis.result.column_edges:
                        frame.columns[edge.target.name] = Column(
                            {(s.dataset_id, s.name) for s in edge.sources},
                            edge.transformation.expression or "",
                            edge.transformation.kind,
                            edge.provenance.confidence != "exact",
                            edge.target.name,
                        )
                        frame.indirect |= {(s.dataset_id, s.name) for s in edge.indirect_sources}
                    frame.partial = bool(frame_analysis.result.unresolved)
                    frame.open_columns = not bool(frame.columns)
                    return frame
                if sink.schema_kw and sink.schema_kw in keywords:
                    namespace = concrete(keywords[sink.schema_kw], kind)
                    if namespace is None:
                        return None
                    value = (
                        f"{namespace}.{value}"
                        if sink.scheme == "table"
                        else f"s3://{namespace}/{value}"
                    )

                def dataset_id(text):
                    """Normalize a resolved path/table string to a canonical dataset id.

                    Args:
                        text (str): The resolved path or table name.

                    Returns:
                        str: ``"file://" + text`` for a bare path-scheme value with no
                        scheme prefix, otherwise the result of
                        :func:`~etl_parser.identity.normalize_dataset_id`.
                    """
                    if sink.scheme == "path" and "://" not in text:
                        return "file://" + text
                    return normalize_dataset_id(text, engine=engine, default_db=self.default_db)

                ds = dataset_id(value)
                if sink.direction == "read":
                    frame = frame_from_dataset(ds)
                    for extra in path_args[1:]:
                        path = concrete(extra, kind)
                        if path is None:
                            frame.partial = True
                            continue
                        other = frame_from_dataset(dataset_id(path))
                        frame.sources |= other.sources
                        for key, column in other.columns.items():
                            if key in frame.columns:
                                frame.columns[key].sources |= column.sources
                            else:
                                frame.columns[key] = column
                        frame.open_columns |= other.open_columns
                    frame.parallel_sources = len(path_args) > 1
                    return frame
                if not isinstance(receiver, Frame) and "df" in keywords:
                    receiver = evaluate(keywords["df"])
                write_frame(receiver, ds, node)
                return receiver

            if name.startswith(
                ("F.", "pl.", "pyspark.sql.functions.", "Window.", "pyspark.sql.Window.")
            ):
                return Expression(node)
            return folded(node)

        def statements(body):
            """Interpret a sequence of statements against the current interpreter state.

            Handles function/class-level definitions (registered for later calls, not
            executed), imports (resolved via ``imported_state``), assignments (including
            subscript assignment onto a tracked :class:`Frame`'s columns), bare expression
            statements, ``return``, ``if``/``else`` (both branches are interpreted from a
            shared starting environment and merged afterward, with divergent values
            downgraded to a :class:`Frame` union or an unknown placeholder), ``with``, a
            bounded ``for`` loop over a literal list/tuple with no ``break``/``continue``/
            ``return`` in its body (interpreted once per item), and other loops/``try``
            blocks (interpreted once conservatively, with every tracked frame marked
            partial afterward). Any exception raised while interpreting one statement is
            caught and recorded as an issue rather than aborting the rest of the body.

            Args:
                body (list): Sequence of statement nodes to interpret.

            Returns:
                The value of a ``return`` statement encountered directly in ``body``, or
                ``None`` when none is reached.
            """
            for node in body:
                try:
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        state.functions[node.name] = node
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            state.imports[alias.asname or alias.name] = (alias.name, 0)
                            module = imported_state(alias.name)
                            if module:
                                state.env[alias.asname or alias.name] = module
                    elif isinstance(node, ast.ImportFrom):
                        for alias in node.names:
                            state.imports[alias.asname or alias.name] = (
                                f"{node.module or ''}.{alias.name}".strip("."),
                                node.level,
                            )
                            module = imported_state(node.module or "", node.level)
                            if module and alias.name in module.env:
                                state.env[alias.asname or alias.name] = module.env[alias.name]
                    elif isinstance(node, (ast.Assign, ast.AnnAssign)):
                        value = evaluate(node.value)
                        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                        for target in targets:
                            if isinstance(target, ast.Name):
                                state.env[target.id] = value
                            elif isinstance(target, ast.Subscript):
                                frame = evaluate(target.value)
                                if isinstance(frame, Frame):
                                    frame.columns[folded(target.slice).text] = col_expr(
                                        node.value, frame
                                    )
                    elif isinstance(node, ast.Expr):
                        evaluate(node.value)
                    elif isinstance(node, ast.Return):
                        return evaluate(node.value)
                    elif isinstance(node, ast.If):
                        if "__name__" in ast.unparse(node.test):
                            statements(node.body)
                        else:
                            original = copy.deepcopy(state.env)
                            statements(node.body)
                            left = state.env
                            state.env = copy.deepcopy(original)
                            statements(node.orelse)
                            for key in set(left) | set(state.env):
                                a, b = left.get(key), state.env.get(key)
                                if a != b:
                                    if isinstance(a, Frame) and isinstance(b, Frame):
                                        a.sources |= b.sources
                                        a.partial = True
                                        state.env[key] = a
                                    else:
                                        state.env[key] = Folded("{{?}}", False, [key])
                    elif isinstance(node, (ast.With, ast.AsyncWith)):
                        statements(node.body)
                    elif (
                        isinstance(node, ast.For)
                        and isinstance(node.target, ast.Name)
                        and isinstance(node.iter, (ast.List, ast.Tuple))
                        and len(node.iter.elts) <= 100
                        and all(isinstance(n, ast.Constant) for n in node.iter.elts)
                        and not any(
                            isinstance(n, (ast.Break, ast.Continue, ast.Return))
                            for child in node.body
                            for n in ast.walk(child)
                        )
                    ):
                        for item in node.iter.elts:
                            state.env[node.target.id] = evaluate(item)
                            statements(node.body)
                        statements(node.orelse)
                    elif isinstance(node, (ast.For, ast.While, ast.Try)):
                        issue(node, "Dynamic control flow analyzed conservatively")
                        statements(node.body)
                        for value in state.env.values():
                            if isinstance(value, Frame):
                                value.partial = True
                except Exception as exc:
                    issue(node, f"{type(exc).__name__}: {exc}")
            return None

        def load_module(source, module_state, definitions_only=False):
            """Parse a module and interpret its body under ``module_state``.

            Args:
                source (SourceFile): The module to parse and interpret.
                module_state (State): The state to interpret the module's body under; made
                    the active ``state`` for the duration of this call.
                definitions_only (bool): When ``True``, only ``Import``, ``ImportFrom``,
                    ``FunctionDef``, ``AsyncFunctionDef``, ``Assign``, and ``AnnAssign``
                    top-level statements are interpreted, so loading a helper module never
                    runs its other top-level side effects.
            """
            nonlocal state
            old, state = state, module_state
            try:
                tree = ast.parse(source.text)
                body = tree.body
                if definitions_only:
                    body = [
                        n
                        for n in body
                        if isinstance(
                            n,
                            (
                                ast.Import,
                                ast.ImportFrom,
                                ast.FunctionDef,
                                ast.AsyncFunctionDef,
                                ast.Assign,
                                ast.AnnAssign,
                            ),
                        )
                    ]
                statements(body)
            except SyntaxError as exc:
                result.unresolved.append(
                    Unresolved(
                        kind="unsupported_syntax",
                        source_file=source.path,
                        line=exc.lineno,
                        reason=str(exc),
                        job_id=job_id,
                    )
                )
            finally:
                state = old

        load_module(source, state, definitions_only=bool(entry_function))
        if entry_function:
            if entry_function not in state.functions:
                matches = [
                    n
                    for n in ast.walk(ast.parse(source.text))
                    if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and n.name == entry_function
                ]
                if len(matches) == 1:
                    state.functions[entry_function] = matches[0]
            function = state.functions.get(entry_function)
            if function:
                invoke(function, [], {}, state)
            else:
                result.unresolved.append(
                    Unresolved(
                        kind="unresolved_import",
                        source_file=source.path,
                        reason=f"Entry function {entry_function!r} not found",
                        job_id=job_id,
                    )
                )
        # Entry functions such as Lambda handlers may be called only by infrastructure.
        for name, function in list(state.functions.items()):
            if (
                not entry_function
                and (source.path, name) not in invoked
                and name in {"main", "handler", "lambda_handler"}
            ):
                invoke(function, [], {}, state)
        header = parse_header(root_source.text)
        job = Job(
            id=job_id,
            name=entry_function or Path(root_source.path).stem,
            source_file=root_source.path,
            language=language,
            engine=default_engine,
            owner=header.get("owner"),
            description=first_docstring_line(root_source.text),
            inputs=sorted(inputs),
            outputs=sorted(outputs),
        )
        if "schedule" in header:
            schedule = comment_schedule(job_id, header["schedule"], root_source.path)
            result.schedules[schedule.id] = schedule
            job.schedule_id = schedule.id
        result.jobs.append(job)
        for ds in sorted(inputs | outputs):
            ref = dataset_ref_from_id(ds)
            ref.columns = sorted(
                {e.target.name for e in result.column_edges if e.target.dataset_id == ds}
                | set(self.sql.schema.columns(ds) or [] if self.sql.schema else [])
            )
            result.datasets.append(ref)
        return result
