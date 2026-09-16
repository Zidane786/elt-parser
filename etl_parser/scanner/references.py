"""Inspect dataset/SQL arguments without importing or executing ETL modules.

Explicit bindings are scan inputs, never values read from the scanner's environment.
This diagnostic pass is independent of the future Python lineage/frame worker.
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


class Reference(BaseModel):
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
    references: list[Reference] = Field(default_factory=list)
    unresolved: list[Unresolved] = Field(default_factory=list)


def inspect_references(
    path: Path,
    *,
    root: Path | None = None,
    bindings: Mapping[str, str] | None = None,
) -> ReferenceReport:
    """Resolve known sink arguments and return actionable diagnostics for runtime names.

    ``bindings={"env:ENV": "prod"}`` supplies an environment value explicitly.
    ``bindings={"env": "prod"}`` supplies an otherwise unknown Python symbol.
    Branch/loop assignments remain uncertain; this pass does not execute control flow.
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
        def __init__(self):
            self.env: dict[str, str | Folded] = dict(bindings or {})
            self.aliases: dict[str, str] = {}

        def visit_Import(self, node):
            for alias in node.names:
                self.aliases[alias.asname or alias.name] = alias.name

        def visit_ImportFrom(self, node):
            for alias in node.names:
                self.aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"

        def callee(self, node):
            text = ast.unparse(node)
            first, dot, rest = text.partition(".")
            text = self.aliases.get(first, first) + (dot + rest if dot else "")
            # Match sink vocabulary after resolving pandas/polars/wrangler import aliases.
            for package, shorthand in (("pandas", "pd"), ("polars", "pl"), ("awswrangler", "wr")):
                if text.startswith(package + "."):
                    return shorthand + text[len(package) :]
            return text

        def visit_Assign(self, node):
            self.visit(node.value)
            value = fold_string(node.value, self.env)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    self.env[target.id] = value
                else:
                    self.invalidate(target)

        def visit_AnnAssign(self, node):
            if node.value is not None:
                self.visit(node.value)
                if isinstance(node.target, ast.Name):
                    self.env[node.target.id] = fold_string(node.value, self.env)

        def invalidate(self, node):
            for item in ast.walk(node):
                if isinstance(item, ast.Name) and isinstance(item.ctx, (ast.Store, ast.Del)):
                    self.env[item.id] = Folded("{{?}}", False, [item.id])

        def visit_AugAssign(self, node):
            self.generic_visit(node)
            self.invalidate(node.target)

        def visit_Delete(self, node):
            self.invalidate(node)

        def visit_FunctionDef(self, node):
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
    """Inspect Python files in deterministic order, excluding generated/vendor directories."""
    report = ReferenceReport()
    ignored = {".git", ".venv", "venv", "node_modules", "__pycache__", "build", "dist"}
    for path in sorted(root.rglob("*.py")):
        if ignored.intersection(path.relative_to(root).parts) or path.is_symlink():
            continue
        partial = inspect_references(path, root=root, bindings=bindings)
        report.references.extend(partial.references)
        report.unresolved.extend(partial.unresolved)
    return report
