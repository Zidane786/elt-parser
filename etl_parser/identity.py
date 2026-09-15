"""Canonical dataset identity.

Rules (spec section 7): the id is ``scheme://namespace/name``. Athena and Spark tables live
in the Glue catalog and share the ``glue`` scheme. Identifiers are lower-cased for glue;
Postgres and MySQL keep case only when the identifier was quoted. Dialect is never part of
the identity: it belongs to the job that executed the statement.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from etl_parser.models import DatasetRef

GLUE_ENGINES = {"athena", "spark", "glue", "trino", "presto", "hive"}
ENGINE_SCHEME = {
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "dynamodb": "dynamodb",
}
URI_SCHEMES = {"s3", "s3a", "s3n", "gs", "hdfs", "file", "kafka", "kinesis", "http", "https"}

_QUOTED = re.compile(r'^\s*(["`\[])(.*)(["`\]])\s*$')


def _split_identifier(name: str) -> list[tuple[str, bool]]:
    """Split ``a.b.c`` into parts, honouring quotes. Returns (text, was_quoted)."""
    parts: list[tuple[str, bool]] = []
    buf = ""
    quote: str | None = None
    quoted = False
    for ch in name:
        if quote:
            if ch == quote or (quote == "[" and ch == "]"):
                quote = None
            else:
                buf += ch
        elif ch in '"`[':
            quote = ch
            quoted = True
        elif ch == ".":
            parts.append((buf, quoted))
            buf, quoted = "", False
        else:
            buf += ch
    parts.append((buf, quoted))
    return [(p.strip(), q) for p, q in parts if p.strip() or q]


def normalize_dataset_id(
    name: str, *, engine: str = "unknown", default_db: str | None = None
) -> str:
    """Return the canonical id for a table name or storage URI as seen at a call site."""
    name = name.strip()
    if "://" in name:
        parsed = urlparse(name)
        scheme = parsed.scheme.lower()
        if scheme in {"s3a", "s3n"}:
            scheme = "s3"
        rest = name.split("://", 1)[1]
        return f"{scheme}://{rest}"
    if name.startswith("/"):
        return f"file://{name}"

    parts = _split_identifier(name)
    engine_l = engine.lower()
    if engine_l in GLUE_ENGINES:
        scheme = "glue"
        parts = [(p.lower(), q) for p, q in parts]
    else:
        scheme = ENGINE_SCHEME.get(engine_l, engine_l if engine_l != "unknown" else "table")
        parts = [(p if q else p.lower(), q) for p, q in parts]

    if len(parts) >= 3:
        # catalog.db.table -> keep db.table, drop the catalog prefix
        parts = parts[-2:]
    if len(parts) == 2:
        db, table = parts[0][0], parts[1][0]
    else:
        table = parts[0][0]
        db = (default_db or "default").lower() if scheme == "glue" else (default_db or "default")
    return f"{scheme}://{db}/{table}"


def split_dataset_id(dataset_id: str) -> tuple[str, str, str]:
    """Return (scheme, namespace, name) for a canonical id."""
    scheme, rest = dataset_id.split("://", 1)
    if scheme in URI_SCHEMES or scheme in {"s3", "file"}:
        bucket, _, path = rest.partition("/")
        return scheme, bucket, path
    ns, _, name = rest.partition("/")
    return scheme, ns, name


def dataset_ref_from_id(dataset_id: str) -> DatasetRef:
    scheme, ns, name = split_dataset_id(dataset_id)
    kind = "s3_path" if scheme == "s3" else "table"
    if scheme in {"api", "http", "https"}:
        kind = "api"
    elif scheme in {"kafka", "kinesis"}:
        kind = scheme  # type: ignore[assignment]
    elif scheme == "file":
        kind = "file"
    return DatasetRef(id=dataset_id, namespace=f"{scheme}://{ns}", name=name, kind=kind)


def agent_table_name(dataset_id: str) -> str:
    """``glue://db/table`` -> ``db.table`` (the shape the agent catalog uses)."""
    scheme, ns, name = split_dataset_id(dataset_id)
    if scheme in URI_SCHEMES or scheme == "s3":
        return dataset_id
    return f"{ns}.{name}"


class DatasetRegistry:
    """Collects DatasetRefs and merges aliases so one physical dataset has one node."""

    def __init__(self) -> None:
        self._refs: dict[str, DatasetRef] = {}
        self._alias_to_canonical: dict[str, str] = {}

    def add(self, ref: DatasetRef) -> DatasetRef:
        canonical_id = self.resolve_id(ref.id)
        existing = self._refs.get(canonical_id)
        if existing is None:
            ref = ref.model_copy(update={"id": canonical_id})
            self._refs[canonical_id] = ref
            return ref
        merged_cols = sorted(set(existing.columns) | set(ref.columns))
        merged_aliases = sorted(
            set(existing.aliases) | set(ref.aliases) | {ref.id} - {canonical_id}
        )
        updated = existing.model_copy(
            update={
                "columns": merged_cols,
                "aliases": merged_aliases,
                "physical_location": existing.physical_location or ref.physical_location,
                "product": existing.product or ref.product,
                "layer": existing.layer or ref.layer,
            }
        )
        self._refs[canonical_id] = updated
        return updated

    def get_or_create(self, dataset_id: str) -> DatasetRef:
        canonical = self.resolve_id(dataset_id)
        if canonical not in self._refs:
            self._refs[canonical] = dataset_ref_from_id(canonical)
        return self._refs[canonical]

    def merge_alias(self, alias_id: str, canonical_id: str) -> None:
        """Declare that ``alias_id`` (e.g. an s3 path) is the same dataset as ``canonical_id``."""
        canonical_id = self.resolve_id(canonical_id)
        alias_id = self.resolve_id(alias_id)
        if alias_id == canonical_id:
            return
        self._alias_to_canonical[alias_id] = canonical_id
        alias_ref = self._refs.pop(alias_id, None)
        target = self.get_or_create(canonical_id)
        aliases = set(target.aliases) | {alias_id}
        update: dict = {"aliases": sorted(aliases)}
        if alias_ref is not None:
            aliases |= set(alias_ref.aliases)
            update["aliases"] = sorted(aliases)
            update["columns"] = sorted(set(target.columns) | set(alias_ref.columns))
            if target.physical_location is None and alias_ref.kind == "s3_path":
                update["physical_location"] = alias_ref.id
        if target.physical_location is None and alias_id.startswith("s3://"):
            update["physical_location"] = alias_id
        self._refs[canonical_id] = target.model_copy(update=update)

    def resolve_id(self, dataset_id: str) -> str:
        seen = set()
        while dataset_id in self._alias_to_canonical and dataset_id not in seen:
            seen.add(dataset_id)
            dataset_id = self._alias_to_canonical[dataset_id]
        return dataset_id

    def all(self) -> list[DatasetRef]:
        return sorted(self._refs.values(), key=lambda r: r.id)
