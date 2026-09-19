"""Inspect dataset/SQL arguments without importing or executing ETL modules.

This is a lightweight, standalone diagnostic pass over sink-matching calls (the same sink
vocabulary used by ``PythonWorker``, design spec section 8.2): for each recognized call it
folds the dataset/SQL argument to a concrete string where possible, and otherwise reports an
``Unresolved`` item explaining why (a runtime value, or a value that depends on an
environment default). Explicit bindings are scan inputs, never values read from the
scanner's environment. This pass performs no DataFrame/column tracking and does not build
lineage edges; it is independent of the full frame-tracking Python worker in
:mod:`etl_parser.workers.python`, which does. The entry points are :func:`inspect_references`
(single file) and :func:`inspect_repository` (whole tree).
"""

from __future__ import annotations

import ast
from collections.abc import Mapping
from pathlib import Path

from pydantic import BaseModel, Field

from etl_parser.models import Unresolved
from etl_parser.scanner.sinks import match_sink
from etl_parser.scanner.strings import Folded, fold_string
from etl_parser.workers.base import job_id_for, repo_relative


def qualified_callee(node: ast.AST, aliases: Mapping[str, str]) -> str:
    """Render a call target's fully qualified name, resolving import aliases.

    Shared by this pass and :class:`etl_parser.workers.python.PythonWorker` so both
    resolve a call site to the same name and therefore to the same sink table entry.

    Args:
        node (ast.AST): The ``Call.func`` expression to render.
        aliases (Mapping[str, str]): Local name to dotted module/callable name, as
            introduced by the file's import statements.

    Returns:
        str: The dotted callee text with its leading name resolved through ``aliases``
        and, for ``pandas``/``polars``/``awswrangler`` targets, rewritten to the
        ``pd.``/``pl.``/``wr.`` shorthand the sink table is written in.
    """
    text = ast.unparse(node)
    first, dot, rest = text.partition(".")
    text = aliases.get(first, first) + (dot + rest if dot else "")
    for package, shorthand in (("pandas", "pd"), ("polars", "pl"), ("awswrangler", "wr")):
        if text.startswith(package + "."):
            return shorthand + text[len(package) :]
    return text


class Reference(BaseModel):
    """One recognized sink call site and the resolved (or partially resolved) value found.

    Attributes:
        source_file (str): Repository-relative path of the file containing the call.
        line (int): Line number of the call.
        callee (str): Fully qualified callee name, after resolving import aliases (for
            example a ``pandas`` import aliased on import is normalized to ``"pd."``).
        direction (str): Whether the call reads or writes data, or carries SQL, as declared
            by the matched sink table entry.
        expression (str): Source text of the argument expression that was inspected.
        value (str): The folded string value, with unresolved parts represented as
            ``{{placeholder}}`` text.
        complete (bool): Whether ``value`` is a fully concrete string with no placeholders.
        symbols (list[str]): Names of symbols that could not be folded to a literal value.
        assumptions (dict[str, str]): Environment-default assumptions made while folding
            (for example an ``os.environ.get`` default that was used as the value).
    """

    source_file: str
    line: int
    callee: str
    direction: str
    expression: str
    value: str
    complete: bool
    symbols: list[str] = Field(default_factory=list)
    assumptions: dict[str, str] = Field(default_factory=dict)


class ReferenceReport(BaseModel):
    """Aggregated output of a references scan: resolved references plus unresolved items.

    Attributes:
        references (list[Reference]): Every recognized sink call found, resolved or not.
        unresolved (list[Unresolved]): Diagnostic items for calls whose dataset/SQL argument
            could not be fully resolved, and for files that failed to read or parse.
    """

    references: list[Reference] = Field(default_factory=list)
    unresolved: list[Unresolved] = Field(default_factory=list)


def inspect_references(
    path: Path,
    *,
    root: Path | None = None,
    bindings: Mapping[str, str] | None = None,
) -> ReferenceReport:
    """Resolve known sink arguments and return actionable diagnostics for runtime names.

    Parses ``path`` and walks it once, tracking simple assignments and import aliases so
    literal, f-string, and environment-default arguments to recognized sink calls (matched
    via :func:`etl_parser.scanner.sinks.match_sink`) can be folded to concrete strings.
    ``bindings={"env:ENV": "prod"}`` supplies an environment value explicitly.
    ``bindings={"env": "prod"}`` supplies an otherwise unknown Python symbol.
    Branch/loop assignments remain uncertain; this pass does not execute control flow.

    Args:
        path (Path): Python file to inspect.
        root (Path | None): Repository root used to compute repository-relative paths and
            the job id; defaults to treating ``path`` itself as the root context.
        bindings (Mapping[str, str] | None): Explicit values for otherwise-unresolvable
            symbols or environment keys, seeded into the folding environment before the
            file is walked.

    Returns:
        ReferenceReport: The references found and any unresolved diagnostics, including a
        single ``unsupported_syntax`` item when the file cannot be read or parsed.
    """
    report = ReferenceReport()
    source_file = repo_relative(path, root)
    job_id = job_id_for(path, root)
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except (OSError, UnicodeError, SyntaxError) as exc:
        report.unresolved.append(
            Unresolved(
                kind="unsupported_syntax",
                source_file=source_file,
                line=getattr(exc, "lineno", None),
                reason=str(exc),
                job_id=job_id,
            )
        )
        return report

    class Visitor(ast.NodeVisitor):
        """Single-pass AST visitor that folds sink-call arguments and records references.

        Tracks a best-effort symbol environment (``env``) and import aliases (``aliases``)
        as it walks the module, so :func:`etl_parser.scanner.strings.fold_string` can
        resolve simple constant and f-string values at each recognized sink call.
        """

        def __init__(self):
            """Initialize the environment from the caller-supplied bindings and no aliases."""
            self.env: dict[str, str | Folded] = dict(bindings or {})
            self.aliases: dict[str, str] = {}

        def visit_Import(self, node):
            """Record aliases introduced by an ``import x`` or ``import x as y`` statement.

            Args:
                node (ast.Import): The import node being visited.
            """
            for alias in node.names:
                self.aliases[alias.asname or alias.name] = alias.name

        def visit_ImportFrom(self, node):
            """Record aliases introduced by a ``from module import name`` statement.

            Args:
                node (ast.ImportFrom): The import-from node being visited.
            """
            for alias in node.names:
                self.aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"

        def callee(self, node):
            """Render a call target's fully qualified name, resolving known import aliases.

            Args:
                node (ast.expr): The ``Call.func`` expression to render.

            Returns:
                str: The dotted callee text, as resolved by :func:`qualified_callee`
                against the aliases collected so far.
            """
            return qualified_callee(node, self.aliases)

        def visit_Assign(self, node):
            """Visit an assignment's value, fold it, and bind or invalidate each target.

            Args:
                node (ast.Assign): The assignment node being visited.
            """
            self.visit(node.value)
            value = fold_string(node.value, self.env)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.env[target.id] = value
                else:
                    self.invalidate(target)

        def visit_AnnAssign(self, node):
            """Visit an annotated assignment and fold its value into the environment.

            Args:
                node (ast.AnnAssign): The annotated assignment node being visited.
            """
            if node.value is not None:
                self.visit(node.value)
                if isinstance(node.target, ast.Name):
                    self.env[node.target.id] = fold_string(node.value, self.env)

        def invalidate(self, node):
            """Mark every name stored or deleted under ``node`` as an unknown placeholder.

            Args:
                node (ast.AST): Subtree whose ``Store``/``Del`` name targets should be
                    treated as no longer having a known concrete value.
            """
            for item in ast.walk(node):
                if isinstance(item, ast.Name) and isinstance(item.ctx, (ast.Store, ast.Del)):
                    self.env[item.id] = Folded("{{?}}", False, [item.id])

        def visit_AugAssign(self, node):
            """Visit an augmented assignment (``x += ...``) and invalidate its target.

            Args:
                node (ast.AugAssign): The augmented assignment node being visited.
            """
            self.generic_visit(node)
            self.invalidate(node.target)

        def visit_Delete(self, node):
            """Visit a ``del`` statement and invalidate the deleted targets.

            Args:
                node (ast.Delete): The delete node being visited.
            """
            self.invalidate(node)

        def visit_FunctionDef(self, node):
            """Visit a function body in its own shadowed environment scope.

            Saves and restores ``self.env``/``self.aliases`` around the visit so local
            bindings (including parameters, treated as unknown placeholders) shadow globals
            only for the duration of this function's body, per Python scoping.

            Args:
                node (ast.FunctionDef): The function definition node being visited.
            """
            old_env, old_aliases = self.env, self.aliases
            self.env, self.aliases = dict(old_env), dict(old_aliases)
            # Local bindings shadow globals throughout a Python function.
            self.invalidate(node)
            args = [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]
            args += [arg for arg in (node.args.vararg, node.args.kwarg) if arg]
            for arg in args:
                self.env[arg.arg] = Folded("{{?}}", False, [arg.arg])
            for statement in node.body:
                self.visit(statement)
            self.env, self.aliases = old_env, old_aliases

        visit_AsyncFunctionDef = visit_FunctionDef

        def visit_control_flow(self, node):
            """Visit a branch/loop/try node, treating each branch as independently uncertain.

            Shared handler for ``If``, ``For``, ``AsyncFor``, ``While``, ``Try``, and
            ``Match``. A possible assignment is not a known runtime value, even if one
            branch happens to be visited last by ``ast.walk``, so every name assigned
            anywhere in ``node`` is invalidated first, then each child is visited against a
            fresh copy of that unknown-shadowed environment (branch effects on ``self.env``
            do not leak into sibling branches).

            Args:
                node (ast.AST): The branching or looping node being visited.
            """
            # A possible assignment is not a known runtime value, even if one branch
            # happens to be visited last by ast.walk.
            self.invalidate(node)
            unknown = dict(self.env)
            for child in ast.iter_child_nodes(node):
                self.env = dict(unknown)
                self.visit(child)
            self.env = unknown

        visit_If = visit_control_flow
        visit_For = visit_control_flow
        visit_AsyncFor = visit_control_flow
        visit_While = visit_control_flow
        visit_Try = visit_control_flow
        visit_Match = visit_control_flow

        def visit_Call(self, node):
            """Match a call against the sink table and record a reference/unresolved item.

            Resolves the callee name, and when it matches a known sink, folds the
            dataset/SQL argument through the current environment. Every match is appended
            to ``report.references``; when the folded value is incomplete or relied on an
            environment-default assumption, a matching ``Unresolved`` item is also appended
            explaining why (dynamic SQL/table name/path) and how to remediate it.

            Args:
                node (ast.Call): The call node being visited.
            """
            callee = self.callee(node.func)
            sink = match_sink(callee)
            if sink:
                keywords = {kw.arg: kw.value for kw in node.keywords if kw.arg}
                arg = (
                    node.args[sink.arg]
                    if isinstance(sink.arg, int) and sink.arg < len(node.args)
                    else keywords.get(sink.arg if isinstance(sink.arg, str) else sink.alt_arg)
                )
                value = fold_string(arg, self.env)
                expression = ast.get_source_segment(source, arg) if arg else "<missing>"
                report.references.append(
                    Reference(
                        source_file=source_file,
                        line=node.lineno,
                        callee=callee,
                        direction=sink.direction,
                        expression=expression or ast.unparse(arg),
                        value=value.text,
                        complete=value.complete,
                        symbols=value.placeholders,
                        assumptions=value.assumptions,
                    )
                )
                if not value.complete or value.assumptions:
                    kind = (
                        "dynamic_sql"
                        if sink.direction == "sql"
                        else "dynamic_path"
                        if sink.scheme in {"path", "s3", "s3_object"}
                        else "dynamic_table_name"
                    )
                    report.unresolved.append(
                        Unresolved(
                            kind=kind,
                            source_file=source_file,
                            line=node.lineno,
                            job_id=job_id,
                            reason=(
                                "Runtime symbols prevent a concrete reference"
                                if not value.complete
                                else "Environment default requires an explicit scan binding"
                            ),
                            partial_text=value.text,
                            expression=expression,
                            symbols=value.placeholders,
                            assumptions=value.assumptions,
                            remediation="Supply bindings for these symbols/environment keys; "
                            "rescan each environment separately.",
                        )
                    )
            self.generic_visit(node)

    Visitor().visit(tree)
    return report


def inspect_repository(root: Path, *, bindings: Mapping[str, str] | None = None) -> ReferenceReport:
    """Inspect Python files in deterministic order, excluding generated/vendor directories.

    Walks ``root`` for ``.py`` files, skipping ``.git``, ``.venv``, ``venv``,
    ``node_modules``, ``__pycache__``, ``build``, ``dist``, and symlinks, and merges each
    file's :func:`inspect_references` result into one combined report.

    Args:
        root (Path): Repository root to walk.
        bindings (Mapping[str, str] | None): Explicit values for otherwise-unresolvable
            symbols or environment keys, passed through to every :func:`inspect_references`
            call.

    Returns:
        ReferenceReport: The combined references and unresolved diagnostics across every
        scanned file, in path-sorted order.
    """
    report = ReferenceReport()
    ignored = {".git", ".venv", "venv", "node_modules", "__pycache__", "build", "dist"}
    for path in sorted(root.rglob("*.py")):
        if ignored.intersection(path.relative_to(root).parts) or path.is_symlink():
            continue
        partial = inspect_references(path, root=root, bindings=bindings)
        report.references.extend(partial.references)
        report.unresolved.extend(partial.unresolved)
    return report
