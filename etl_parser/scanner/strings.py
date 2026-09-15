"""Reconstruct string values from Python AST nodes without executing code.

Literal parts join directly. Names resolve through an environment of module-level constants.
Anything else becomes a ``{{?}}`` placeholder and the result is marked incomplete, so callers
can decide whether the unknown part affects table or column identity.
"""

from __future__ import annotations

import ast
from collections.abc import Mapping
from dataclasses import dataclass, field

PLACEHOLDER = "{{?}}"


@dataclass
class Folded:
    text: str
    complete: bool
    placeholders: list[str] = field(default_factory=list)


def _name_of(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # pragma: no cover
        return type(node).__name__


def fold_string(node: ast.AST | None, env: Mapping[str, str] | None = None) -> Folded:
    env = env or {}
    if node is None:
        return Folded(PLACEHOLDER, False, ["<missing>"])

    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            return Folded(node.value, True)
        if isinstance(node.value, (int, float, bool)) or node.value is None:
            return Folded(str(node.value), True)
        return Folded(PLACEHOLDER, False, [repr(node.value)])

    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        complete = True
        holes: list[str] = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                inner = fold_string(v.value, env)
                parts.append(inner.text)
                complete &= inner.complete
                holes.extend(inner.placeholders)
        return Folded("".join(parts), complete, holes)

    if isinstance(node, ast.Name):
        if node.id in env:
            return Folded(env[node.id], True)
        return Folded(PLACEHOLDER, False, [node.id])

    if isinstance(node, ast.BinOp):
        if isinstance(node.op, ast.Add):
            left, right = fold_string(node.left, env), fold_string(node.right, env)
            return Folded(
                left.text + right.text,
                left.complete and right.complete,
                left.placeholders + right.placeholders,
            )
        if isinstance(node.op, ast.Mod):
            template = fold_string(node.left, env)
            values = node.right.elts if isinstance(node.right, ast.Tuple) else [node.right]
            return _apply_percent(template, [fold_string(v, env) for v in values])

    if isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "format":
            template = fold_string(func.value, env)
            positional = [fold_string(a, env) for a in node.args]
            keywords = {kw.arg: fold_string(kw.value, env) for kw in node.keywords if kw.arg}
            return _apply_format(template, positional, keywords)
        if isinstance(func, ast.Attribute) and func.attr == "join" and node.args:
            sep = fold_string(func.value, env)
            seq = node.args[0]
            if isinstance(seq, (ast.List, ast.Tuple)):
                items = [fold_string(e, env) for e in seq.elts]
                return Folded(
                    sep.text.join(i.text for i in items),
                    sep.complete and all(i.complete for i in items),
                    [p for i in items for p in i.placeholders],
                )
        if isinstance(func, ast.Attribute) and func.attr in {"strip", "lower", "upper"}:
            inner = fold_string(func.value, env)
            text = getattr(inner.text, func.attr)() if inner.complete else inner.text
            return Folded(text, inner.complete, inner.placeholders)
        env_default = _environ_default(node, env)
        if env_default is not None:
            return env_default
        if isinstance(func, ast.Name) and func.id == "str" and node.args:
            return fold_string(node.args[0], env)

    if isinstance(node, ast.Subscript):
        env_default = _environ_default(node, env)
        if env_default is not None:
            return env_default

    return Folded(PLACEHOLDER, False, [_name_of(node)])


def _environ_default(node: ast.AST, env: Mapping[str, str]) -> Folded | None:
    """``os.environ.get("X", "dflt")`` / ``os.getenv("X", "dflt")`` -> default value.

    ``os.environ["X"]`` has no default and stays a placeholder that names the variable.
    """
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        base = ast.unparse(node.func.value)
        if (node.func.attr == "get" and base in {"os.environ", "environ"}) or (
            node.func.attr == "getenv" and base == "os"
        ):
            key = fold_string(node.args[0], env).text if node.args else "?"
            if len(node.args) > 1:
                return fold_string(node.args[1], env)
            return Folded(PLACEHOLDER, False, [f"env:{key}"])
    if isinstance(node, ast.Subscript) and ast.unparse(node.value) in {"os.environ", "environ"}:
        key = fold_string(node.slice, env).text
        return Folded(PLACEHOLDER, False, [f"env:{key}"])
    return None


def _apply_percent(template: Folded, values: list[Folded]) -> Folded:
    if not template.complete:
        return Folded(
            template.text,
            False,
            template.placeholders + [p for v in values for p in v.placeholders],
        )
    out = template.text
    holes = list(template.placeholders)
    complete = True
    for v in values:
        idx = out.find("%")
        if idx == -1:
            break
        end = idx + 1
        while end < len(out) and out[end] not in "sdifr%":
            end += 1
        out = out[:idx] + v.text + out[end + 1 :]
        complete &= v.complete
        holes.extend(v.placeholders)
    return Folded(out, complete, holes)


def _apply_format(
    template: Folded, positional: list[Folded], keywords: dict[str, Folded]
) -> Folded:
    if not template.complete:
        return Folded(template.text, False, template.placeholders)
    out = template.text
    complete = True
    holes: list[str] = []
    auto = 0
    result = ""
    i = 0
    while i < len(out):
        ch = out[i]
        if ch == "{" and i + 1 < len(out) and out[i + 1] == "{":
            result += "{"
            i += 2
            continue
        if ch == "}" and i + 1 < len(out) and out[i + 1] == "}":
            result += "}"
            i += 2
            continue
        if ch == "{":
            close = out.find("}", i)
            if close == -1:
                result += out[i:]
                break
            spec = out[i + 1 : close].split(":")[0].split("!")[0]
            value: Folded | None
            if spec == "":
                value = positional[auto] if auto < len(positional) else None
                auto += 1
            elif spec.isdigit():
                value = positional[int(spec)] if int(spec) < len(positional) else None
            else:
                value = keywords.get(spec)
            if value is None:
                result += PLACEHOLDER
                complete = False
                holes.append(spec or f"arg{auto}")
            else:
                result += value.text
                complete &= value.complete
                holes.extend(value.placeholders)
            i = close + 1
            continue
        result += ch
        i += 1
    return Folded(result, complete, holes)


def collect_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level ``NAME = <foldable>`` assignments, resolved in order."""
    env: dict[str, str] = {}
    for stmt in tree.body:
        targets: list[ast.expr] = []
        value: ast.expr | None = None
        if isinstance(stmt, ast.Assign):
            targets, value = stmt.targets, stmt.value
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            targets, value = [stmt.target], stmt.value
        for t in targets:
            if isinstance(t, ast.Name) and value is not None:
                folded = fold_string(value, env)
                if folded.complete:
                    env[t.id] = folded.text
    return env
