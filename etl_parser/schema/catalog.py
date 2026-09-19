"""Merge several schema sources into one deterministic ``catalog.json``.

The output has exactly the shape of the user's ``fetch_glue_schema.py`` reference script
(``{"databases": [...], "relations": [...]}``) so that
:class:`~etl_parser.workers.sql.DictSchemaProvider` and the agent catalog exporter read it
unchanged.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Iterable
from pathlib import Path

from etl_parser.schema.base import SchemaSource, relation_key, sort_databases


def merge_databases(catalogs: Iterable[dict]) -> list[dict]:
    """Merge the ``databases`` lists of several catalogs.

    Databases with the same ``(db_name, db_type)`` are folded together; a table that
    appears in several sources keeps the first source's definition.

    Args:
        catalogs: ``{"databases": [...]}`` dicts, in source order.

    Returns:
        list[dict]: Deep-copied, merged and deterministically sorted databases.
    """
    merged: dict[tuple[str, str], dict] = {}
    for catalog in catalogs:
        for database in catalog.get("databases", []):
            key = (database.get("db_name", ""), database.get("db_type", ""))
            target = merged.get(key)
            if target is None:
                merged[key] = copy.deepcopy(database)
                merged[key].setdefault("tables", [])
                continue
            if not target.get("description") and database.get("description"):
                target["description"] = database["description"]
            known = {t["table_name"] for t in target["tables"]}
            for table in database.get("tables", []):
                if table["table_name"] not in known:
                    target["tables"].append(copy.deepcopy(table))
                    known.add(table["table_name"])
    return sort_databases(list(merged.values()))


def merge_relations(relation_lists: Iterable[Iterable[dict]]) -> list[dict]:
    """Deduplicate and sort relations, stamping ``source: "database"`` on each.

    Args:
        relation_lists: Relation lists from each source.

    Returns:
        list[dict]: Sorted, deduplicated relation dicts.
    """
    seen: dict[tuple, dict] = {}
    for relations in relation_lists:
        for relation in relations:
            entry = dict(relation)
            entry.setdefault("source", "database")
            seen.setdefault(relation_key(entry), entry)
    return [seen[k] for k in sorted(seen)]


def write_schema_catalog(sources: Iterable[SchemaSource], out_path: str | Path | None = None):
    """Fetch every source and merge them into one catalog dict, optionally written to disk.

    Args:
        sources: Schema sources to fetch (:meth:`~SchemaSource.catalog` and
            :meth:`~SchemaSource.relations` are called once each).
        out_path: File to write the JSON to (parents created). Nothing is written when
            ``None``.

    Returns:
        dict: ``{"databases": [...], "relations": [...]}``, deterministically ordered and
        free of connection details.
    """
    sources = list(sources)
    catalog = {
        "databases": merge_databases(s.catalog() for s in sources),
        "relations": merge_relations(s.relations() for s in sources),
    }
    if out_path is not None:
        path = Path(out_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(catalog, indent=2, ensure_ascii=False, default=str) + "\n",
            encoding="utf-8",
        )
    return catalog
