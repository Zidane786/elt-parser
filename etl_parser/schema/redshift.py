"""Amazon Redshift schema source over ``svv_columns``, ``svv_table_info`` and ``pg_catalog``.

Connection settings come only from the ``dsn`` argument or the environment
(``ETL_PARSER_REDSHIFT_DSN``, else ``REDSHIFT_HOST``/``REDSHIFT_PORT``/
``REDSHIFT_DATABASE``/``REDSHIFT_USER``/``REDSHIFT_PASSWORD``; with ``iam=True`` also
``REDSHIFT_CLUSTER_IDENTIFIER``/``REDSHIFT_DB_USER``). ``redshift_connector`` is imported
lazily. Schema filters are validated as identifiers and inlined as quoted literals
because the driver does not bind array parameters.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict

from etl_parser.schema.base import SchemaSourceError, parse_dsn
from etl_parser.schema.postgres import PostgresSchemaSource

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_SYSTEM = "('pg_catalog', 'pg_internal', 'information_schema', 'pg_automv', 'pg_toast')"

SQL_COLUMNS = """
SELECT table_schema, table_name, column_name, data_type, ordinal_position
FROM svv_columns
WHERE table_schema NOT IN {system}{filter:table_schema}
ORDER BY table_schema, table_name, ordinal_position
"""

SQL_TABLES = """
SELECT "schema", "table"
FROM svv_table_info
WHERE "schema" NOT IN {system}{filter:"schema"}
ORDER BY 1, 2
"""

SQL_TABLE_COMMENTS = """
SELECT n.nspname, c.relname, d.description
FROM pg_catalog.pg_description d
JOIN pg_catalog.pg_class c ON c.oid = d.objoid AND d.objsubid = 0
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v'){filter:n.nspname}
"""

SQL_COLUMN_COMMENTS = """
SELECT n.nspname, c.relname, a.attname, d.description
FROM pg_catalog.pg_description d
JOIN pg_catalog.pg_class c ON c.oid = d.objoid AND d.objsubid > 0
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = d.objsubid
WHERE 1 = 1{filter:n.nspname}
"""

SQL_CONSTRAINTS = """
SELECT n.nspname, c.relname, con.conname, con.contype, pg_get_constraintdef(con.oid)
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE con.contype IN ('p', 'u', 'f'){filter:n.nspname}
ORDER BY 1, 2, 3
"""

_CONSTRAINT = re.compile(
    r"^(?P<kind>PRIMARY KEY|UNIQUE|FOREIGN KEY)\s*\((?P<cols>[^)]*)\)"
    r"(?:\s*REFERENCES\s+(?P<ref>[^(]+)\((?P<refcols>[^)]*)\))?",
    re.IGNORECASE,
)


def _idents(text: str) -> list[str]:
    """Split a comma-separated identifier list, stripping quotes and whitespace."""
    return [part.strip().strip('"') for part in text.split(",") if part.strip()]


def _env_settings() -> tuple[dict[str, str], dict[str, str]]:
    """Read Redshift connection settings from the environment.

    Returns:
        tuple: ``(dsn_kwargs, plain_kwargs)``: the parsed ``ETL_PARSER_REDSHIFT_DSN`` (or
        ``{}``) and the ``REDSHIFT_*`` variables mapped to driver keyword names.
    """
    dsn = os.environ.get("ETL_PARSER_REDSHIFT_DSN") or ""
    names = {
        "REDSHIFT_HOST": "host",
        "REDSHIFT_PORT": "port",
        "REDSHIFT_DATABASE": "database",
        "REDSHIFT_USER": "user",
        "REDSHIFT_PASSWORD": "password",
        "REDSHIFT_CLUSTER_IDENTIFIER": "cluster_identifier",
        "REDSHIFT_DB_USER": "db_user",
    }
    plain = {key: os.environ[var] for var, key in names.items() if os.environ.get(var)}
    return (_dsn_kwargs(dsn) if dsn else {}), plain


def _dsn_kwargs(dsn: str) -> dict[str, str]:
    """Map a DSN to ``redshift_connector.connect`` keyword names."""
    parts = parse_dsn(dsn)
    if "dbname" in parts:
        parts["database"] = parts.pop("dbname")
    return parts


class RedshiftSchemaSource(PostgresSchemaSource):
    """Source of truth backed by an Amazon Redshift cluster or serverless workgroup.

    Attributes:
        schemas: Optional schema names to restrict to (``None`` means every non-system
            schema).
        iam: Whether to authenticate through IAM (``GetClusterCredentials``) using the
            AWS profile/region instead of a database password.
    """

    db_type = "redshift"
    scheme = "redshift"
    extra = "redshift"
    driver = "redshift_connector"

    def __init__(
        self,
        dsn: str | None = None,
        *,
        schemas: list[str] | None = None,
        iam: bool = False,
        profile: str | None = None,
        region: str | None = None,
        cluster_identifier: str | None = None,
        db_user: str | None = None,
        connection=None,
    ):
        """Create a Redshift schema source; no connection is opened until first use.

        Args:
            dsn: ``redshift://user:password@host:port/database`` or ``key=value`` string.
                When ``None`` the environment is used (see the module docstring).
            schemas: Restrict to these schema names (validated as identifiers).
            iam: Use IAM authentication; the password is then never needed.
            profile: AWS profile for IAM authentication.
            region: AWS region for IAM authentication.
            cluster_identifier: Cluster identifier for IAM authentication (falls back to
                ``REDSHIFT_CLUSTER_IDENTIFIER``).
            db_user: Database user to obtain temporary credentials for (falls back to
                ``REDSHIFT_DB_USER``, then the DSN/``REDSHIFT_USER`` user).
            connection: An already-open DB-API connection (real or fake); when given no
                driver is imported and the other settings are ignored.

        Raises:
            SchemaSourceError: If a schema name is not a plain SQL identifier.
        """
        super().__init__(dsn, schemas=schemas, connection=connection)
        for name in self.schemas or []:
            if not _IDENTIFIER.match(name):
                raise SchemaSourceError(f"Schema name {name!r} is not a plain SQL identifier")
        self.iam = iam
        self._profile = profile
        self._region = region
        self._cluster_identifier = cluster_identifier
        self._db_user = db_user

    def _driver(self):
        """Import ``redshift_connector`` lazily.

        Raises:
            ImportError: Naming the ``redshift`` extra when the driver is missing.
        """
        try:
            import redshift_connector  # type: ignore[import-not-found]
        except ImportError as exc:
            raise ImportError(
                f"{type(self).__name__} requires the {self.driver!r} package; install the "
                f"{self.extra!r} extra: pip install 'etl-parser[{self.extra}]'"
            ) from exc
        return redshift_connector

    def _open(self, dsn: str | None):
        """Open the connection with password or IAM authentication.

        Args:
            dsn: The constructor's DSN, or ``None`` to use the environment.

        Raises:
            SchemaSourceError: When settings are missing or the connection fails (only the
                error type is reported).
        """
        env_dsn, plain = _env_settings()
        kwargs = _dsn_kwargs(dsn) if dsn else env_dsn
        for key, value in plain.items():
            kwargs.setdefault(key, value)
        if "port" in kwargs:
            kwargs["port"] = int(kwargs["port"])
        if self.iam:
            kwargs.pop("password", None)
            kwargs["iam"] = True
            if self._profile:
                kwargs["profile"] = self._profile
            if self._region:
                kwargs["region"] = self._region
            cluster = self._cluster_identifier or kwargs.get("cluster_identifier")
            if cluster:
                kwargs["cluster_identifier"] = cluster
            db_user = self._db_user or kwargs.get("db_user") or kwargs.get("user")
            if db_user:
                kwargs["db_user"] = db_user
        else:
            kwargs.pop("cluster_identifier", None)
            kwargs.pop("db_user", None)
        if not kwargs.get("database") or not (
            kwargs.get("host") or kwargs.get("cluster_identifier")
        ):
            raise SchemaSourceError(
                "No Redshift connection settings; pass dsn= or set ETL_PARSER_REDSHIFT_DSN or "
                "REDSHIFT_HOST/REDSHIFT_PORT/REDSHIFT_DATABASE/REDSHIFT_USER/REDSHIFT_PASSWORD "
                "(REDSHIFT_CLUSTER_IDENTIFIER/REDSHIFT_DB_USER with iam=True)"
            )
        driver = self._driver()
        try:
            return driver.connect(**kwargs)
        except Exception as exc:
            raise SchemaSourceError(
                f"Could not connect to Redshift ({type(exc).__name__}); check the connection "
                "settings in the environment"
            ) from None

    def _sql(self, template: str) -> str:
        """Render a query template: system schema list and optional schema filter."""

        def render(match):
            """Expand one schema-filter placeholder into a literal IN list, or nothing."""
            column = match.group(1)
            if not self.schemas:
                return ""
            literals = ", ".join("'" + name + "'" for name in self.schemas)
            return f" AND {column} IN ({literals})"

        return re.sub(r"\{filter:([^}]+)\}", render, template.replace("{system}", _SYSTEM))

    def _rows(self, sql: str) -> list[tuple]:
        """Run one rendered catalog query; the driver binds no array parameters."""
        if self._connection is None:
            self._connection = self._opener()
        cursor = self._connection.cursor()
        cursor.execute(self._sql(sql))
        return list(cursor.fetchall())

    def _fetch_columns(self) -> list[tuple]:
        """Return ``(schema, table, column, datatype, ordinal)`` rows.

        Tables listed by ``svv_table_info`` but without ``svv_columns`` rows (for example
        when the user lacks column privileges) are kept as empty tables.
        """
        rows = [tuple(r[:5]) for r in self._rows(SQL_COLUMNS)]
        seen = {(r[0], r[1]) for r in rows}
        for schema, table in self._rows(SQL_TABLES):
            if (schema, table) not in seen:
                rows.append((schema, table, None, None, 0))
        return rows

    def _fetch_table_comments(self) -> dict[tuple[str, str], str]:
        """Return ``{(schema, table): comment}``."""
        return {(r[0], r[1]): r[2] or "" for r in self._rows(SQL_TABLE_COMMENTS)}

    def _fetch_column_comments(self) -> dict[tuple[str, str, str], str]:
        """Return ``{(schema, table, column): comment}``."""
        return {(r[0], r[1], r[2]): r[3] or "" for r in self._rows(SQL_COLUMN_COMMENTS)}

    def _constraints(self) -> list[tuple]:
        """Parse constraint definitions into ``(schema, table, kind, cols, ref, refcols)``."""
        out = []
        for schema, table, _name, _type, definition in self._rows(SQL_CONSTRAINTS):
            match = _CONSTRAINT.match((definition or "").strip())
            if not match:
                continue
            kind = match.group("kind").upper()
            cols = _idents(match.group("cols"))
            ref = match.group("ref")
            refcols = _idents(match.group("refcols") or "")
            if ref is not None:
                parts = _idents(ref.replace(".", ","))
                ref = (parts[-2], parts[-1]) if len(parts) > 1 else (schema, parts[-1])
            out.append((schema, table, kind, cols, ref, refcols))
        return out

    def _fetch_keys(self) -> list[tuple[str, str, str, list[str]]]:
        """Return ``(schema, table, constraint_type, [columns])`` for PK/UNIQUE constraints."""
        return [
            (schema, table, kind, cols)
            for schema, table, kind, cols, _ref, _refcols in self._constraints()
            if kind in ("PRIMARY KEY", "UNIQUE")
        ]

    def _fetch_foreign_keys(self) -> list[tuple]:
        """Return ``(child_schema, child_table, [child_cols], parent_schema, parent_table,
        [parent_cols])`` per foreign-key constraint."""
        grouped: dict[tuple, list] = defaultdict(list)
        for schema, table, kind, cols, ref, refcols in self._constraints():
            if kind == "FOREIGN KEY" and ref and len(refcols) == len(cols):
                grouped[(schema, table, ref[0], ref[1])].append((cols, refcols))
        out = []
        for (cs, ct, ps, pt), pairs in sorted(grouped.items()):
            for cols, refcols in pairs:
                out.append((cs, ct, cols, ps, pt, refcols))
        return out
