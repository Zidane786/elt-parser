"""Shared contract and helpers for source-of-truth schema fetchers.

A :class:`SchemaSource` answers the same ``columns(dataset_id)`` question as the older
``SchemaProvider`` protocol in ``etl_parser.workers.sql`` and additionally exposes the
whole catalog it knows about in the agent ``databases`` shape (the contract set by the
user's ``fetch_glue_schema.py`` reference script), the foreign-key relations the system
declares, and alias hints between logical and physical ids. Nothing in this module imports
a database driver or an AWS SDK; concrete sources import them lazily.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import unquote, urlsplit


class SchemaSourceError(RuntimeError):
    """A schema source could not be read; the message never contains credentials."""


@runtime_checkable
class SchemaSource(Protocol):
    """Pluggable source of truth for databases, tables, columns and relations.

    Concrete implementations: :class:`~etl_parser.schema.glue.GlueSchemaSource`,
    :class:`~etl_parser.schema.postgres.PostgresSchemaSource` and
    :class:`~etl_parser.schema.redshift.RedshiftSchemaSource`. Every method is
    deterministic for a fixed source state: lists are sorted before they are returned.
    """

    def columns(self, dataset_id: str) -> list[str] | None:
        """Return the known column names of a dataset, or ``None`` when unknown.

        Args:
            dataset_id: Canonical ``scheme://namespace/name`` id (see ``identity.py``).

        Returns:
            Ordered column names, or ``None`` if the dataset is not in this source.
        """
        ...

    def catalog(self) -> dict:
        """Return ``{"databases": [...]}`` in the agent catalog shape.

        Each database is ``{db_name, db_type, description, tables[]}`` and each table is
        ``{table_name, description, schema[] {field_name, datatype, description,
        is_partition?}, location?}``.
        """
        ...

    def relations(self) -> list[dict]:
        """Return declared foreign-key relations.

        Each item is ``{from_table, to_table, from_column, to_column, relation_type,
        source: "database"}`` where ``from_table`` is the referenced (parent) table and
        ``to_table`` the referencing (child) table, both as ``<db_name>.<table_name>``.
        """
        ...

    def aliases(self) -> list[tuple[str, str]]:
        """Return ``(logical_id, physical_id)`` pairs known to name the same dataset.

        For Glue this is ``("glue://db/table", "s3://bucket/prefix/")``; sources without
        physical locations return an empty list.
        """
        ...


def is_schema_source(value: Any) -> bool:
    """Return whether ``value`` implements the full :class:`SchemaSource` protocol.

    Args:
        value: Any object; plain ``SchemaProvider`` instances (``columns`` only) are not
            schema sources.

    Returns:
        bool: True when ``value`` has ``columns``, ``catalog``, ``relations`` and
        ``aliases`` methods.
    """
    return isinstance(value, SchemaSource)


def as_schema_provider(schema):
    """Coerce a scan ``schema=`` argument into an object with a ``columns`` method.

    Args:
        schema: ``None``, an object with ``columns`` (a ``SchemaProvider`` or
            :class:`SchemaSource`), a catalog-shaped or ``{db: {table: [cols]}}``
            mapping, or a ``str``/``Path`` to such a JSON file.

    Returns:
        The provider to hand to the SQL worker, or ``None`` when ``schema`` is ``None``.
    """
    if schema is None or hasattr(schema, "columns"):
        return schema
    if isinstance(schema, (Mapping, str, Path)):
        from etl_parser.workers.sql import DictSchemaProvider

        return DictSchemaProvider(schema)
    raise TypeError(f"Unsupported schema argument of type {type(schema).__name__}")


def column_entry(name: str, datatype: str | None, description: str | None, *, partition=False):
    """Build one ``schema[]`` entry in the reference output shape.

    Args:
        name: Column name.
        datatype: Column type as reported by the source (``""`` when unknown).
        description: Column comment (``""`` when none).
        partition: Whether the column is a partition key; adds ``is_partition: True``.

    Returns:
        dict: ``{field_name, datatype, description}`` plus ``is_partition`` when set.
    """
    entry = {"field_name": name, "datatype": datatype or "", "description": description or ""}
    if partition:
        entry["is_partition"] = True
    return entry


def sort_databases(databases: list[dict]) -> list[dict]:
    """Sort databases and their tables in place for deterministic output.

    Column order is preserved: sources report it in ordinal order, which carries meaning.

    Args:
        databases: Catalog ``databases[]`` entries.

    Returns:
        list[dict]: The same list, sorted by ``(db_name, db_type)`` then ``table_name``.
    """
    for database in databases:
        database["tables"].sort(key=lambda t: t["table_name"])
    databases.sort(key=lambda d: (d["db_name"], d.get("db_type", "")))
    return databases


def relation_key(relation: Mapping) -> tuple:
    """Return the deterministic sort/dedupe key of a relation dict.

    Args:
        relation: A relation entry.

    Returns:
        tuple: ``(from_table, from_column, to_table, to_column, relation_type)``.
    """
    return tuple(
        str(relation.get(k, ""))
        for k in ("from_table", "from_column", "to_table", "to_column", "relation_type")
    )


_KV = re.compile(r"(\w+)\s*=\s*(?:'([^']*)'|(\S+))")
_KEYS = {"host": "host", "hostaddr": "host", "port": "port", "dbname": "dbname", "user": "user"}


def parse_dsn(dsn: str) -> dict[str, str]:
    """Split a URL- or ``key=value``-style DSN into connection keyword arguments.

    Args:
        dsn: ``scheme://user:password@host:port/dbname`` or ``host=... dbname=...``.

    Returns:
        dict[str, str]: Subset of ``host``, ``port``, ``dbname``, ``user``, ``password``.
    """
    if "://" in dsn:
        parts = urlsplit(dsn)
        out = {
            "host": parts.hostname or "",
            "port": str(parts.port or ""),
            "dbname": parts.path.lstrip("/"),
            "user": unquote(parts.username or ""),
            "password": unquote(parts.password or ""),
        }
    else:
        out = {}
        for key, quoted, bare in _KV.findall(dsn):
            value = quoted if quoted else bare
            if key == "password":
                out["password"] = value
            elif key in _KEYS:
                out[_KEYS[key]] = value
    return {k: v for k, v in out.items() if v}
