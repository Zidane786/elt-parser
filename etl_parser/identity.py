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

UNRESOLVED_SCHEME = "unknown"
UNRESOLVED_NAMESPACE = "unresolved"
UNRESOLVED_PREFIX = f"{UNRESOLVED_SCHEME}://{UNRESOLVED_NAMESPACE}/"
"""Prefix of the sentinel id used when a reference cannot be named at all (finding 27).

Identity helpers never raise on degenerate input: an empty or punctuation-only name, or an
id with no scheme, resolves to a sentinel so the caller can record an ``Unresolved`` item
instead of aborting a whole scan.
"""

_UNSAFE = re.compile(r"[^A-Za-z0-9_.\-]")


def unresolved_dataset_id(text: str) -> str:
    """Return the sentinel id standing in for a reference that could not be named.

    Args:
        text: The original text, used only to make the sentinel readable; characters
            outside ``[A-Za-z0-9_.-]`` are replaced and leading/trailing dots removed.

    Returns:
        str: ``"unknown://unresolved/<sanitised>"``, or ``"unknown://unresolved/empty"``
        when nothing usable remains. The result is deterministic for a given input.
    """
    sanitised = _UNSAFE.sub("_", text.strip()).strip(".") or "empty"
    return f"{UNRESOLVED_PREFIX}{sanitised}"


def is_unresolved_dataset_id(dataset_id: str) -> bool:
    """Report whether an id is the sentinel produced for an unnameable reference.

    Args:
        dataset_id: Any dataset id.

    Returns:
        bool: True when the id was produced by :func:`unresolved_dataset_id`.
    """
    return dataset_id.startswith(UNRESOLVED_PREFIX)


def scheme_for_engine(engine: str | None) -> str | None:
    """Return the dataset id scheme an engine's tables are addressed under.

    Args:
        engine: Engine name as declared by a product or a call site (e.g. ``"athena"``,
            ``"postgresql"``), or ``None``.

    Returns:
        str | None: ``"glue"`` for the Glue-catalog family (Athena, Spark, Glue, Trino,
        Presto, Hive), the mapped scheme for other known engines, the engine name itself
        for unknown engines, and ``None`` when ``engine`` is ``None`` or empty.
    """
    if not engine:
        return None
    engine = engine.lower()
    if engine in GLUE_ENGINES:
        return "glue"
    return ENGINE_SCHEME.get(engine, engine)


def _split_identifier(name: str) -> list[tuple[str, bool]]:
    """Split a dotted identifier into its parts, honoring quoted segments.

    Args:
        name: A possibly dotted, possibly quoted identifier, e.g. ``a.b."c.d"``.

    Returns:
        list[tuple[str, bool]]: One ``(text, was_quoted)`` pair per non-empty part, in
        order.
    """
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
    """Return the canonical id for a table name or storage URI as seen at a call site.

    Applies the identity rules in spec section 7: URIs and absolute paths pass through
    with their scheme normalized; two- and three-part SQL names resolve to ``glue`` for
    Athena/Spark/Hive-family engines (lower-cased) or to the engine's own scheme
    otherwise (case preserved only when quoted); one-part names resolve against
    ``default_db``.

    Args:
        name: The identifier or URI text as it appeared at the call site.
        engine: Executing engine (e.g. ``athena``, ``spark``, ``postgres``); determines the
            resulting scheme and case-folding behavior.
        default_db: Database to use when ``name`` has no database part.

    Returns:
        str: The canonical ``scheme://namespace/name`` id, or the
        ``unknown://unresolved/...`` sentinel when ``name`` holds no usable identifier
        (finding 27); this function never raises on degenerate input.
    """
    name = name.strip()
    if not name:
        return unresolved_dataset_id(name)
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
    if not [p for p, _ in parts if p]:
        return unresolved_dataset_id(name)
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
    """Split a canonical id into its scheme, namespace, and name.

    Args:
        dataset_id: A canonical ``scheme://namespace/name`` id.

    Returns:
        tuple[str, str, str]: ``(scheme, namespace, name)``. For URI-style schemes (e.g.
        ``s3``, ``file``) the namespace is the bucket/host and the name is the remaining
        path. An id with no scheme yields ``("unknown", "unresolved", dataset_id)`` rather
        than raising (finding 27), so a malformed id reported by a worker cannot abort a
        caller mid-scan.
    """
    if "://" not in dataset_id:
        return UNRESOLVED_SCHEME, UNRESOLVED_NAMESPACE, dataset_id
    scheme, rest = dataset_id.split("://", 1)
    if scheme in URI_SCHEMES or scheme in {"s3", "file"}:
        bucket, _, path = rest.partition("/")
        return scheme, bucket, path
    ns, _, name = rest.partition("/")
    return scheme, ns, name


def dataset_ref_from_id(dataset_id: str) -> DatasetRef:
    """Build a minimal :class:`DatasetRef` from a canonical id alone.

    Used when a dataset is referenced (e.g. by an edge) but no richer :class:`DatasetRef`
    for it has been registered yet.

    Args:
        dataset_id: Canonical ``scheme://namespace/name`` id.

    Returns:
        DatasetRef: A ref with ``namespace``, ``name``, and ``kind`` inferred from the
        scheme, and no aliases or columns.
    """
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
    """Render a canonical id in the ``db.table`` shape the agent catalog format uses.

    Args:
        dataset_id: A canonical ``scheme://namespace/name`` id.

    Returns:
        str: ``dataset_id`` unchanged for URI-style schemes (e.g. ``s3``), otherwise
        ``"namespace.name"``.
    """
    scheme, ns, name = split_dataset_id(dataset_id)
    if scheme in URI_SCHEMES or scheme == "s3":
        return dataset_id
    return f"{ns}.{name}"


class DatasetRegistry:
    """Collects DatasetRefs and merges aliases so one physical dataset has one node.

    Used by the graph builder to deduplicate datasets discovered under different names
    (e.g. a Glue table and the S3 path it is backed by) into a single canonical node.
    """

    def __init__(self) -> None:
        """Create an empty registry with no datasets or aliases."""
        self._refs: dict[str, DatasetRef] = {}
        self._alias_to_canonical: dict[str, str] = {}

    def add(self, ref: DatasetRef) -> DatasetRef:
        """Register a dataset, merging it into any existing entry with the same canonical id.

        Args:
            ref: The dataset to register. Its own ``id`` is treated as an alias if it
                differs from its resolved canonical id.

        Returns:
            DatasetRef: The stored (possibly merged) dataset, with columns, aliases, and
            optional fields unioned with any prior entry.
        """
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
        """Return the registered dataset for an id, creating a minimal one if needed.

        Args:
            dataset_id: A canonical or alias id.

        Returns:
            DatasetRef: The existing entry for this id's canonical form, or a new minimal
            ref (via :func:`dataset_ref_from_id`) registered and returned if none existed.
        """
        canonical = self.resolve_id(dataset_id)
        if canonical not in self._refs:
            self._refs[canonical] = dataset_ref_from_id(canonical)
        return self._refs[canonical]

    def merge_alias(self, alias_id: str, canonical_id: str) -> None:
        """Declare that ``alias_id`` (e.g. an s3 path) is the same dataset as ``canonical_id``.

        Any existing entry under ``alias_id`` is folded into the canonical entry: its
        columns and aliases are unioned in, and it may fill in a missing
        ``physical_location`` on the canonical entry.

        Args:
            alias_id: The id to merge away.
            canonical_id: The id ``alias_id`` should resolve to from now on.
        """
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
        """Follow alias chains to the canonical id for a dataset.

        Args:
            dataset_id: A canonical or alias id.

        Returns:
            str: The canonical id, or ``dataset_id`` unchanged if it has no alias mapping.
        """
        seen = set()
        while dataset_id in self._alias_to_canonical and dataset_id not in seen:
            seen.add(dataset_id)
            dataset_id = self._alias_to_canonical[dataset_id]
        return dataset_id

    def all(self) -> list[DatasetRef]:
        """Return every registered dataset, sorted by canonical id.

        Returns:
            list[DatasetRef]: All registered datasets, sorted by ``id``.
        """
        return sorted(self._refs.values(), key=lambda r: r.id)
