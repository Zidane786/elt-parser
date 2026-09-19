"""PostgreSQL schema source over ``information_schema`` and ``pg_catalog``.

Connection settings come only from the ``dsn`` argument or the environment
(``ETL_PARSER_POSTGRES_DSN``, else ``PGHOST``/``PGPORT``/``PGDATABASE``/``PGUSER``/
``PGPASSWORD``). The DSN is never stored on the instance, logged, serialized or echoed
in an exception. ``psycopg`` is imported lazily and only when no connection is injected.
"""

from __future__ import annotations

import os
from collections import defaultdict

from etl_parser.identity import split_dataset_id
from etl_parser.schema.base import (
    SchemaSourceError,
    column_entry,
    relation_key,
    sort_databases,
)

SQL_COLUMNS = """
SELECT table_schema, table_name, column_name, data_type, ordinal_position
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND (%(schemas)s::text[] IS NULL OR table_schema = ANY(%(schemas)s::text[]))
ORDER BY table_schema, table_name, ordinal_position
"""

SQL_TABLE_COMMENTS = """
SELECT n.nspname, c.relname, d.description
FROM pg_catalog.pg_description d
JOIN pg_catalog.pg_class c ON c.oid = d.objoid AND d.objsubid = 0
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm', 'p', 'f')
  AND (%(schemas)s::text[] IS NULL OR n.nspname = ANY(%(schemas)s::text[]))
"""

SQL_COLUMN_COMMENTS = """
SELECT n.nspname, c.relname, a.attname, d.description
FROM pg_catalog.pg_description d
JOIN pg_catalog.pg_class c ON c.oid = d.objoid AND d.objsubid > 0
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
WHERE (%(schemas)s::text[] IS NULL OR n.nspname = ANY(%(schemas)s::text[]))
"""

SQL_KEYS = """
SELECT tc.constraint_schema, tc.constraint_name, tc.constraint_type,
       tc.table_schema, tc.table_name, kcu.column_name, kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_schema = tc.constraint_schema AND kcu.constraint_name = tc.constraint_name
WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
  AND (%(schemas)s::text[] IS NULL OR tc.table_schema = ANY(%(schemas)s::text[]))
ORDER BY 1, 2, 7
"""

SQL_FOREIGN_KEYS = """
SELECT rc.constraint_schema, rc.constraint_name,
       child.table_schema, child.table_name, child.column_name,
       parent.table_schema, parent.table_name, parent.column_name,
       child.ordinal_position
FROM information_schema.referential_constraints rc
JOIN information_schema.key_column_usage child
  ON child.constraint_schema = rc.constraint_schema
 AND child.constraint_name = rc.constraint_name
JOIN information_schema.key_column_usage parent
  ON parent.constraint_schema = rc.unique_constraint_schema
 AND parent.constraint_name = rc.unique_constraint_name
 AND parent.ordinal_position = child.position_in_unique_constraint
WHERE (%(schemas)s::text[] IS NULL OR child.table_schema = ANY(%(schemas)s::text[]))
ORDER BY 1, 2, 9
"""


def _env_settings() -> tuple[str | None, dict[str, str]]:
    """Read connection settings from the environment.

    Returns:
        tuple: ``(dsn, kwargs)`` where ``dsn`` is ``ETL_PARSER_POSTGRES_DSN`` if set and
        ``kwargs`` the ``PG*`` variables mapped to ``psycopg.connect`` keyword names.
    """
    dsn = os.environ.get("ETL_PARSER_POSTGRES_DSN") or None
    names = {
        "PGHOST": "host",
        "PGPORT": "port",
        "PGDATABASE": "dbname",
        "PGUSER": "user",
        "PGPASSWORD": "password",
    }
    kwargs = {key: os.environ[var] for var, key in names.items() if os.environ.get(var)}
    return dsn, kwargs


class PostgresSchemaSource:
    """Source of truth backed by a PostgreSQL database.

    Attributes:
        schemas: Optional schema names to restrict to (``None`` means every non-system
            schema).
    """

    db_type = "postgresql"
    scheme = "postgres"
    extra = "postgres"
    driver = "psycopg"

    def __init__(
        self, dsn: str | None = None, *, schemas: list[str] | None = None, connection=None
    ):
        """Create a Postgres schema source; no connection is opened until first use.

        Args:
            dsn: libpq connection string or URL. When ``None`` the environment is used
                (``ETL_PARSER_POSTGRES_DSN``, else ``PGHOST``/``PGPORT``/``PGDATABASE``/
                ``PGUSER``/``PGPASSWORD``).
            schemas: Restrict to these schema names.
            connection: An already-open DB-API connection (real or fake); when given no
                driver is imported and ``dsn`` is ignored.
        """
        # Kept in a closure, not an attribute, so repr()/vars() can never expose it.
        self._opener = lambda: self._open(dsn)
        self.schemas = list(schemas) if schemas else None
        self._connection = connection
        self._loaded: dict | None = None

    def __repr__(self) -> str:
        """Return a representation that never includes connection details."""
        return f"{type(self).__name__}(schemas={self.schemas!r})"

    def _driver(self):
        """Import the database driver lazily.

        Raises:
            ImportError: Naming the optional extra to install when the driver is missing.
        """
        try:
            import psycopg  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                f"{type(self).__name__} requires the {self.driver!r} package; install the "
                f"{self.extra!r} extra: pip install 'etl-parser[{self.extra}]'"
            ) from exc
        return psycopg

    def _open(self, dsn: str | None):
        """Open the connection from the argument or environment settings.

        Args:
            dsn: The constructor's DSN, or ``None`` to use the environment.

        Raises:
            SchemaSourceError: When no connection settings are available, or the
                connection attempt fails (the message names the error type only).
        """
        env_dsn, kwargs = _env_settings()
        dsn = dsn or env_dsn
        if not dsn and not kwargs.get("host") and not kwargs.get("dbname"):
            raise SchemaSourceError(
                "No PostgreSQL connection settings; pass dsn= or set ETL_PARSER_POSTGRES_DSN "
                "or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD"
            )
        driver = self._driver()
        try:
            return driver.connect(dsn) if dsn else driver.connect(**kwargs)
        except Exception as exc:
            raise SchemaSourceError(
                f"Could not connect to PostgreSQL ({type(exc).__name__}); check the "
                "connection settings in the environment"
            ) from None

    def _rows(self, sql: str) -> list[tuple]:
        """Run one catalog query with the schema filter bound as a parameter."""
        if self._connection is None:
            self._connection = self._opener()
        cursor = self._connection.cursor()
        cursor.execute(sql, {"schemas": self.schemas})
        return list(cursor.fetchall())

    # -- raw fetches, overridden by engine-specific subclasses ---------------------------

    def _fetch_columns(self) -> list[tuple]:
        """Return ``(schema, table, column, datatype, ordinal)`` rows."""
        return [tuple(r[:5]) for r in self._rows(SQL_COLUMNS)]

    def _fetch_table_comments(self) -> dict[tuple[str, str], str]:
        """Return ``{(schema, table): comment}``."""
        return {(r[0], r[1]): r[2] or "" for r in self._rows(SQL_TABLE_COMMENTS)}

    def _fetch_column_comments(self) -> dict[tuple[str, str, str], str]:
        """Return ``{(schema, table, column): comment}``."""
        return {(r[0], r[1], r[2]): r[3] or "" for r in self._rows(SQL_COLUMN_COMMENTS)}

    def _fetch_keys(self) -> list[tuple[str, str, str, list[str]]]:
        """Return ``(schema, table, constraint_type, [columns])`` for PK/UNIQUE constraints."""
        grouped: dict[tuple, list[str]] = defaultdict(list)
        kinds: dict[tuple, tuple[str, str, str]] = {}
        for cschema, cname, ctype, tschema, tname, column, _pos in self._rows(SQL_KEYS):
            grouped[(cschema, cname)].append(column)
            kinds[(cschema, cname)] = (tschema, tname, ctype)
        return [(*kinds[k], cols) for k, cols in sorted(grouped.items())]

    def _fetch_foreign_keys(self) -> list[tuple]:
        """Return ``(child_schema, child_table, [child_cols], parent_schema, parent_table,
        [parent_cols])`` per foreign-key constraint."""
        grouped: dict[tuple, tuple[list[str], list[str]]] = {}
        tables: dict[tuple, tuple] = {}
        for row in self._rows(SQL_FOREIGN_KEYS):
            cschema, cname, cs, ct, ccol, ps, pt, pcol, _pos = row
            child_cols, parent_cols = grouped.setdefault((cschema, cname), ([], []))
            child_cols.append(ccol)
            parent_cols.append(pcol)
            tables[(cschema, cname)] = (cs, ct, ps, pt)
        out = []
        for key in sorted(grouped):
            cs, ct, ps, pt = tables[key]
            child_cols, parent_cols = grouped[key]
            out.append((cs, ct, child_cols, ps, pt, parent_cols))
        return out

    # -- assembly --------------------------------------------------------------------------

    def _load(self) -> dict:
        """Fetch everything once and build the catalog, relations and column index."""
        if self._loaded is not None:
            return self._loaded
        table_comments = self._fetch_table_comments()
        column_comments = self._fetch_column_comments()
        tables: dict[tuple[str, str], list[tuple]] = defaultdict(list)
        for schema, table, column, datatype, ordinal in self._fetch_columns():
            cols = tables[(schema, table)]  # a ``None`` column registers an empty table
            if column is not None:
                cols.append((ordinal, column, datatype))
        databases: dict[str, dict] = {}
        index: dict[str, list[str]] = {}
        for (schema, table), cols in sorted(tables.items()):
            cols.sort()
            entry = {
                "table_name": table,
                "description": table_comments.get((schema, table), ""),
                "schema": [
                    column_entry(c, t, column_comments.get((schema, table, c), ""))
                    for _o, c, t in cols
                ],
            }
            databases.setdefault(
                schema,
                {"db_name": schema, "db_type": self.db_type, "description": "", "tables": []},
            )["tables"].append(entry)
            index[f"{schema}.{table}".lower()] = [c for _o, c, _t in cols]
        unique = {
            (schema, table, tuple(sorted(cols)))
            for schema, table, _kind, cols in self._fetch_keys()
        }
        relations: dict[tuple, dict] = {}
        for cs, ct, child_cols, ps, pt, parent_cols in self._fetch_foreign_keys():
            kind = "one_to_one" if (cs, ct, tuple(sorted(child_cols))) in unique else "one_to_many"
            for child_col, parent_col in zip(child_cols, parent_cols, strict=True):
                entry = {
                    "from_table": f"{ps}.{pt}",
                    "to_table": f"{cs}.{ct}",
                    "from_column": parent_col,
                    "to_column": child_col,
                    "relation_type": kind,
                    "source": "database",
                }
                relations.setdefault(relation_key(entry), entry)
        self._loaded = {
            "databases": sort_databases(list(databases.values())),
            "relations": [relations[k] for k in sorted(relations)],
            "index": index,
        }
        return self._loaded

    def catalog(self) -> dict:
        """Return ``{"databases": [...]}`` with one database per schema.

        Returns:
            dict: Databases (``db_name`` = schema name, ``db_type`` = engine type) sorted
            by name; columns in ordinal order with table and column comments.
        """
        return {"databases": self._load()["databases"]}

    def relations(self) -> list[dict]:
        """Return foreign keys as ``one_to_many`` (or ``one_to_one``) relations.

        Returns:
            list[dict]: One relation per referencing/referenced column pair, parent table
            as ``from_table`` and child table as ``to_table`` (``<schema>.<table>``),
            sorted and stamped ``source: "database"``.
        """
        return list(self._load()["relations"])

    def columns(self, dataset_id: str) -> list[str] | None:
        """Return a table's columns for ``<scheme>://<schema>/<table>`` ids.

        Args:
            dataset_id: Canonical dataset id; only this engine's scheme is answered.

        Returns:
            Column names in ordinal order, or ``None`` for other schemes or unknown tables.
        """
        scheme, schema, table = split_dataset_id(dataset_id)
        if scheme != self.scheme:
            return None
        return self._load()["index"].get(f"{schema}.{table}".lower())

    def aliases(self) -> list[tuple[str, str]]:
        """Relational tables have no separate physical id; always empty."""
        return []
