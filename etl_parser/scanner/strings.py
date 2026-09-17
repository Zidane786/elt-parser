"""Reconstruct string values from Python AST nodes without executing code.

Literal parts join directly. Names resolve through an environment of module-level constants.
Anything else becomes a ``{{?}}`` placeholder and the result is marked incomplete, so callers
can decide whether the unknown part affects table or column identity.
"""

from __future__ import annotations

import ast
import re
import string
from collections.abc import Mapping
from dataclasses import dataclass, field

PLACEHOLDER = "{{?}}"


@dataclass
class Folded:
    text: str
    complete: bool
    placeholders: list[str] = field(default_factory=list)
    assumptions: dict[str, str] = field(default_factory=dict)
    value_type: str = "str"


def _native(value: Folded):
    if value.value_type == "int":
        return int(value.text)
    if value.value_type == "float":
        return float(value.text)
    if value.value_type == "bool":
        return value.text == "True"
    if value.value_type == "NoneType":
        return None
    return value.text


def _bounded_spec(spec: str) -> bool:
    return all(int(n) <= 10000 for n in re.findall(r"\d+", spec))


def _combine(text: str, values: list[Folded]) -> Folded:
    return Folded(
        text,
        all(v.complete for v in values),
        list(dict.fromkeys(p for v in values for p in v.placeholders)),
        {k: value for v in values for k, value in v.assumptions.items()},
    )


def _name_of(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # pragma: no cover
        return type(node).__name__


def fold_string(node: ast.AST | None, env: Mapping[str, str | Folded] | None = None) -> Folded:
    env = env or {}
    if node is None:
        return Folded(PLACEHOLDER, False, ["<missing>"])

    if isinstance(node, ast.Constant):
        if isinstance(node.value, str):
            return Folded(node.value, True)
        if isinstance(node.value, (int, float, bool)) or node.value is None:
            return Folded(str(node.value), True, value_type=type(node.value).__name__)
        return Folded(PLACEHOLDER, False, [repr(node.value)])

    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        values: list[Folded] = []
        for v in node.values:
            if isinstance(v, ast.Constant):
                parts.append(str(v.value))
            elif isinstance(v, ast.FormattedValue):
                inner = fold_string(v.value, env)
                spec = fold_string(v.format_spec, env) if v.format_spec else Folded("", True)
                if inner.complete and spec.complete:
                    try:
                        value = _native(inner)
                        if not _bounded_spec(spec.text):
                            raise ValueError("format width exceeds analysis limit")
                        if v.conversion == ord("r"):
                            value = repr(value)
                        elif v.conversion == ord("a"):
                            value = ascii(value)
                        elif v.conversion == ord("s"):
                            value = str(value)
                        inner = _combine(format(value, spec.text), [inner, spec])
                    except (ValueError, TypeError):
                        inner = Folded(PLACEHOLDER, False, [_name_of(v)])
                elif not spec.complete:
                    inner = _combine(PLACEHOLDER, [inner, spec])
                parts.append(inner.text)
                values.append(inner)
        return _combine("".join(parts), values)

    if isinstance(node, (ast.Name, ast.Attribute)):
        key = _name_of(node)
        if key in env:
            value = env[key]
            return value if isinstance(value, Folded) else Folded(value, True)
        return Folded(PLACEHOLDER, False, [key])

    if isinstance(node, ast.BinOp):
        if isinstance(node.op, ast.Add):
            left, right = fold_string(node.left, env), fold_string(node.right, env)
            if left.complete and right.complete:
                try:
                    value = _native(left) + _native(right)
                    result = _combine(str(value), [left, right])
                    result.value_type = type(value).__name__
                    return result
                except TypeError:
                    return Folded(PLACEHOLDER, False, [_name_of(node)])
            return _combine(left.text + right.text, [left, right])
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
                return _combine(sep.text.join(i.text for i in items), [sep, *items])
        if isinstance(func, ast.Attribute) and func.attr in {"strip", "lower", "upper"}:
            inner = fold_string(func.value, env)
            args = [fold_string(a, env) for a in node.args]
            try:
                text = (
                    getattr(inner.text, func.attr)(*[a.text for a in args])
                    if inner.complete and all(a.complete for a in args)
                    else PLACEHOLDER
                )
            except TypeError:
                return Folded(PLACEHOLDER, False, [_name_of(node)])
            return _combine(text, [inner, *args])
        env_default = _environ_default(node, env)
        if env_default is not None:
            return env_default
        if isinstance(func, ast.Name) and func.id == "str" and node.args:
            value = fold_string(node.args[0], env)
            return _combine(value.text, [value])

    if isinstance(node, ast.Subscript):
        env_default = _environ_default(node, env)
        if env_default is not None:
            return env_default

    return Folded(PLACEHOLDER, False, [_name_of(node)])


def _environ_default(node: ast.AST, env: Mapping[str, str | Folded]) -> Folded | None:
    """``os.environ.get("X", "dflt")`` / ``os.getenv("X", "dflt")`` -> default value.

    ``os.environ["X"]`` has no default and stays a placeholder that names the variable.
    """
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        base = ast.unparse(node.func.value)
        if (node.func.attr == "get" and base in {"os.environ", "environ"}) or (
            node.func.attr == "getenv" and base == "os"
        ):
            key = fold_string(node.args[0], env).text if node.args else "?"
            if f"env:{key}" in env:
                value = env[f"env:{key}"]
                return value if isinstance(value, Folded) else Folded(value, True)
            default = (
                node.args[1]
                if len(node.args) > 1
                else next((kw.value for kw in node.keywords if kw.arg == "default"), None)
            )
            if default is not None:
                value = fold_string(default, env)
                return Folded(
                    value.text,
                    value.complete,
                    value.placeholders,
                    {**value.assumptions, f"env:{key}": value.text},
                )
            return Folded(PLACEHOLDER, False, [f"env:{key}"])
    if isinstance(node, ast.Subscript) and ast.unparse(node.value) in {"os.environ", "environ"}:
        key = fold_string(node.slice, env).text
        if f"env:{key}" in env:
            value = env[f"env:{key}"]
            return value if isinstance(value, Folded) else Folded(value, True)
        return Folded(PLACEHOLDER, False, [f"env:{key}"])
    return None


def _apply_percent(template: Folded, values: list[Folded]) -> Folded:
    if not template.complete:
        return Folded(
            template.text,
            False,
            template.placeholders + [p for v in values for p in v.placeholders],
        )
    if all(value.complete for value in values):
        try:
            if any(
                not _bounded_spec(spec)
                for spec in re.findall(r"%[-+ #0\d.*]*[a-zA-Z]", template.text)
            ):
                raise ValueError("format width exceeds analysis limit")
            return _combine(
                template.text % tuple(_native(value) for value in values), [template, *values]
            )
        except (ValueError, TypeError, OverflowError):
            return Folded(PLACEHOLDER, False, [template.text])
    parts: list[str] = []
    position = 0
    index = 0
    for match in re.finditer(r"%", template.text):
        if match.start() < position:
            continue
        parts.append(template.text[position : match.start()])
        token = re.match(r"%(?:%|s)", template.text[match.start() :])
        if token is None:
            return Folded(PLACEHOLDER, False, [template.text])
        position = match.start() + len(token.group())
        if token.group() == "%%":
            parts.append("%")
        elif index < len(values):
            parts.append(values[index].text)
            index += 1
        else:
            return Folded(PLACEHOLDER, False, [template.text])
    if index != len(values):
        return Folded(PLACEHOLDER, False, [template.text])
    parts.append(template.text[position:])
    return _combine("".join(parts), [template, *values])


def _apply_format(
    template: Folded, positional: list[Folded], keywords: dict[str, Folded]
) -> Folded:
    if not template.complete:
        return Folded(template.text, False, template.placeholders)
    used = [template]
    auto = 0
    parts = []
    try:
        for literal, name, spec, conversion in string.Formatter().parse(template.text):
            parts.append(literal)
            if name is None:
                continue
            if name == "":
                value = positional[auto] if auto < len(positional) else None
                auto += 1
            elif name.isdigit():
                value = positional[int(name)] if int(name) < len(positional) else None
            else:
                value = keywords.get(name)
            if value is None:
                value = Folded(PLACEHOLDER, False, [name or f"arg{auto}"])
            if value.complete and (spec or conversion):
                try:
                    if not _bounded_spec(spec):
                        raise ValueError("format width exceeds analysis limit")
                    converted = string.Formatter().convert_field(_native(value), conversion)
                    value = _combine(format(converted, spec), [value])
                except (ValueError, TypeError):
                    value = Folded(PLACEHOLDER, False, [name or f"arg{auto}"])
            parts.append(value.text)
            used.append(value)
    except ValueError:
        return Folded(PLACEHOLDER, False, [template.text])
    return _combine("".join(parts), used)


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
                else:
                    env.pop(t.id, None)
    return env
