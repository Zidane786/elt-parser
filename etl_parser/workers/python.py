"""Static frame and I/O analysis shared by PySpark, Pandas and Polars.

The interpreter handles a deliberately finite set of AST/frame operations. It never
imports ETL code. Unknown frame operations preserve known sources with partial confidence.
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
    node: ast.AST


@dataclass
class Frame:
    columns: dict[str, Column] = field(default_factory=dict)
    sources: set[str] = field(default_factory=set)
    aliases: dict[str, Frame] = field(default_factory=dict)
    indirect: set[tuple[str, str]] = field(default_factory=set)
    partial: bool = False
    open_columns: bool = True
    group: list[str] = field(default_factory=list)

    def column(self, name: str) -> Column:
        alias, dot, tail = name.partition(".")
        if dot and alias in self.aliases:
            return self.aliases[alias].column(tail)
        if name in self.columns:
            return copy.deepcopy(self.columns[name])
        if self.open_columns and len(self.sources) == 1 and not dot:
            return Column({(next(iter(self.sources)), name)}, name, name=name)
        return Column(text=name, partial=True, name=name)


@dataclass
class Connection:
    engine: str


class PythonWorker:
    def __init__(
        self,
        sql_worker: SqlWorker | None = None,
        index: ScanIndex | None = None,
        *,
        bindings: dict[str, str] | None = None,
        default_db: str | None = None,
        max_import_depth: int = 3,
    ):
        self.sql = sql_worker or SqlWorker()
        self.index = index
        self.bindings = bindings or {}
        self.default_db = default_db
        self.max_import_depth = max_import_depth

    def analyze_file(self, path: Path) -> WorkerResult:
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
        result = WorkerResult()
        inputs: set[str] = set()
        outputs: set[str] = set()
        job_id = job_id_override or source.job_id
        root_source = source
        language = "pyspark" if "pyspark" in source.text else "python"
        default_engine = "spark" if language == "pyspark" else "unknown"
        invoked: set[tuple[str, str]] = set()
        active: set[tuple[str, str]] = set()

        def issue(node, reason, kind="unsupported_syntax", folded=None):
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
            source: SourceFile
            env: dict = field(default_factory=lambda: dict(self.bindings))
            imports: dict = field(default_factory=dict)
            functions: dict = field(default_factory=dict)
            depth: int = 0

        state = State(source)

        def strings():
            return {k: v for k, v in state.env.items() if isinstance(v, (str, Folded))}

        def folded(node):
            return fold_string(node, strings())

        def concrete(node, kind):
            value = folded(node)
            if not value.complete or value.assumptions:
                issue(
                    node, "Reference depends on runtime values or environment defaults", kind, value
                )
                return None
            return value.text

        def callee(node):
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
            column = _col_expr(node, frame)
            if column.source_file is None:
                column.source_file = state.source.path
                column.line_start = getattr(node, "lineno", None)
                column.line_end = getattr(node, "end_lineno", column.line_start)
            return column

        def _col_expr(node, frame):
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
            inputs.add(ds)
            columns = self.sql.schema.columns(ds) if self.sql.schema else None
            return Frame(
                {c: Column({(ds, c)}, c, name=c) for c in columns or []},
                {ds},
                open_columns=columns is None,
            )

        def write_frame(frame, ds, node):
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

        def evaluate(node):
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
                base = evaluate(node.value)
                return base if node.attr in {"write", "read", "loc", "iloc"} else folded(node)
            if not isinstance(node, ast.Call):
                return folded(node)
            name = callee(node.func)
            method = (
                node.func.attr if isinstance(node.func, ast.Attribute) else name.rsplit(".", 1)[-1]
            )
            keywords = {kw.arg: kw.value for kw in node.keywords if kw.arg}
            if method in {"create_engine", "connect"} and node.args:
                url = folded(node.args[0]).text
                engine = next(
                    (value for key, value in URL_ENGINE_HINTS.items() if key in url), "unknown"
                )
                return Connection(engine)
            receiver = evaluate(node.func.value) if isinstance(node.func, ast.Attribute) else None
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
                elif method in {
                    "mode",
                    "partitionBy",
                    "format",
                    "option",
                    "options",
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
                    "toDF",
                }:
                    pass
                else:
                    out.partial = True
                    issue(node, f"Frame method {method!r} has no handler", "unknown_column")
                return out

            sink = match_sink(name)
            if sink:
                arg = (
                    node.args[sink.arg]
                    if isinstance(sink.arg, int) and sink.arg < len(node.args)
                    else keywords.get(sink.arg if isinstance(sink.arg, str) else sink.alt_arg)
                )
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
                value = concrete(arg, kind)
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
                ds = normalize_dataset_id(value, engine=engine, default_db=self.default_db)
                if sink.direction == "read":
                    return frame_from_dataset(ds)
                write_frame(receiver, ds, node)
                return receiver

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
                function_name = (
                    method
                    if isinstance(node.func, ast.Attribute)
                    else module_name.rsplit(".", 1)[-1]
                )
                lookup = (
                    module_name
                    if isinstance(node.func, ast.Attribute)
                    else module_name.rsplit(".", 1)[0]
                )
                helper = self.index.resolve_module(lookup, state.source, level)
                if helper:
                    old = state
                    args = [evaluate(a) for a in node.args]
                    kwargs = {k: evaluate(v) for k, v in keywords.items()}
                    module_state = State(helper, depth=state.depth + 1)
                    load_module(helper, module_state, definitions_only=True)
                    function = module_state.functions.get(function_name)
                    if function:
                        return invoke(function, args, kwargs, module_state)
                    issue(
                        node,
                        f"Helper {function_name!r} not found in {helper.path}",
                        "unresolved_import",
                    )
                elif lookup in self.index.module_map:
                    issue(node, f"Ambiguous helper import: {lookup}", "unresolved_import")
            if name.startswith(
                ("F.", "pl.", "pyspark.sql.functions.", "Window.", "pyspark.sql.Window.")
            ):
                return Expression(node)
            return folded(node)

        def statements(body):
            for node in body:
                try:
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        state.functions[node.name] = node
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            state.imports[alias.asname or alias.name] = (alias.name, 0)
                    elif isinstance(node, ast.ImportFrom):
                        for alias in node.names:
                            state.imports[alias.asname or alias.name] = (
                                f"{node.module or ''}.{alias.name}".strip("."),
                                node.level,
                            )
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
