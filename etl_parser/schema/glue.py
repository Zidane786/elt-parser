"""AWS Glue Data Catalog schema source (boto3, imported only when a client is needed).

Mirrors the user's ``fetch_glue_schema.py`` reference script: paginated
``get_databases``/``get_tables``, partition keys flagged ``is_partition``, ``db_type``
``"athena"``, and an optional ``.env`` load when ``python-dotenv`` is importable. The
table cache is shared between :meth:`GlueSchemaSource.catalog` and
:meth:`GlueSchemaSource.columns`, so a scan that already qualified columns does not fetch
the same table twice.
"""

from __future__ import annotations

from etl_parser.identity import normalize_dataset_id, split_dataset_id
from etl_parser.schema.base import SchemaSourceError, column_entry, sort_databases

_PERMISSIONS = {
    "get_databases": "glue:GetDatabases",
    "get_database": "glue:GetDatabase",
    "get_tables": "glue:GetTables",
    "get_table": "glue:GetTable",
}


class _NotFound(Exception):
    """Internal marker: Glue reported ``EntityNotFoundException``."""


class GlueSchemaSource:
    """Source of truth backed by the Glue Data Catalog.

    Attributes:
        databases: Optional explicit database names to fetch (``None`` means all).
    """

    def __init__(
        self,
        session=None,
        *,
        profile: str | None = None,
        region: str | None = None,
        databases: list[str] | None = None,
        client=None,
    ):
        """Create a Glue schema source; nothing is imported or connected until first use.

        Args:
            session: An existing ``boto3.Session``; built lazily from ``profile``/``region``
                and the standard credential chain when ``None``.
            profile: Named AWS profile for the lazily built session.
            region: AWS region for the lazily built session; falls back to the SDK
                defaults (``AWS_DEFAULT_REGION``, config file) when omitted.
            databases: Restrict fetching to these Glue database names.
            client: An existing Glue client (real or fake); overrides ``session``.
        """
        self._session = session
        self._profile = profile
        self._region = region
        self.databases = list(databases) if databases else None
        self._client = client
        self._tables: dict[tuple[str, str], dict | None] = {}
        self._catalog: dict | None = None

    @property
    def client(self):
        """The Glue client, created on first access from the session/profile/region."""
        if self._client is None:
            session = self._session
            if session is None:
                try:
                    from dotenv import load_dotenv  # type: ignore[import-not-found]

                    load_dotenv()
                except ImportError:
                    pass
                import boto3  # type: ignore[import-untyped]

                kwargs = {}
                if self._profile:
                    kwargs["profile_name"] = self._profile
                if self._region:
                    kwargs["region_name"] = self._region
                session = boto3.Session(**kwargs)
            self._client = session.client("glue")
        return self._client

    def _call(self, operation: str, **kwargs):
        """Invoke a Glue API and translate provider errors into safe exceptions.

        Args:
            operation: Client method name, e.g. ``"get_tables"``.
            **kwargs: Forwarded to the client method.

        Returns:
            The API response dict.

        Raises:
            SchemaSourceError: On access denial (names the IAM permission) or any other
                provider error (names only the error code, never the raw message).
            _NotFound: When Glue reports ``EntityNotFoundException``.
        """
        try:
            return getattr(self.client, operation)(**kwargs)
        except Exception as exc:
            code = (
                str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
                or type(exc).__name__
            )
            if "EntityNotFound" in code:
                raise _NotFound() from None
            target = kwargs.get("DatabaseName") or kwargs.get("Name") or ""
            where = f" on {target!r}" if target else ""
            if "AccessDenied" in code:
                raise SchemaSourceError(
                    f"Access denied calling {operation}{where}; grant the "
                    f"{_PERMISSIONS.get(operation, operation)} permission"
                ) from None
            raise SchemaSourceError(f"Glue {operation}{where} failed with {code}") from None

    def _paginate(self, operation: str, key: str, **kwargs) -> list[dict]:
        """Collect every page of a list API by following ``NextToken``."""
        items: list[dict] = []
        params = dict(kwargs)
        while True:
            response = self._call(operation, **params)
            items.extend(response.get(key, []))
            token = response.get("NextToken")
            if not token:
                return items
            params["NextToken"] = token

    def _list_databases(self) -> list[tuple[str, str]]:
        """Return ``(name, description)`` for the selected databases, sorted by name."""
        if self.databases is None:
            rows = self._paginate("get_databases", "DatabaseList")
            found = [(d["Name"], d.get("Description", "")) for d in rows]
        else:
            found = []
            for name in self.databases:
                try:
                    database = self._call("get_database", Name=name).get("Database", {})
                except _NotFound:
                    raise SchemaSourceError(
                        f"Glue database {name!r} not found in the Data Catalog"
                    ) from None
                found.append((name, database.get("Description", "")))
        return sorted(found)

    def _remember(self, database: str, table: dict, name: str | None = None) -> None:
        """Store a fetched table in the shared cache under ``(database, table)`` lower-cased."""
        self._tables[(database.lower(), (name or table.get("Name", "")).lower())] = table

    @staticmethod
    def _columns_of(table: dict) -> list[dict]:
        """Build ``schema[]`` entries: storage columns first, then partition keys."""
        out = [
            column_entry(c.get("Name", ""), c.get("Type", ""), c.get("Comment", ""))
            for c in table.get("StorageDescriptor", {}).get("Columns", [])
        ]
        out += [
            column_entry(c.get("Name", ""), c.get("Type", ""), c.get("Comment", ""), partition=True)
            for c in table.get("PartitionKeys", [])
        ]
        return out

    def catalog(self) -> dict:
        """Fetch (once) every selected database and return ``{"databases": [...]}``.

        Returns:
            dict: Databases sorted by name with ``db_type: "athena"``; tables sorted by
            name, each with ``location`` when Glue reports a storage location.

        Raises:
            SchemaSourceError: When a database is missing or an API call is denied.
        """
        if self._catalog is None:
            databases = []
            for name, description in self._list_databases():
                try:
                    tables = self._paginate("get_tables", "TableList", DatabaseName=name)
                except _NotFound:
                    raise SchemaSourceError(
                        f"Glue database {name!r} not found in the Data Catalog"
                    ) from None
                entries = []
                for table in tables:
                    self._remember(name, table)
                    entry = {
                        "table_name": table.get("Name", ""),
                        "description": table.get("Description", ""),
                        "schema": self._columns_of(table),
                    }
                    location = table.get("StorageDescriptor", {}).get("Location")
                    if location:
                        entry["location"] = location
                    entries.append(entry)
                databases.append(
                    {
                        "db_name": name,
                        "db_type": "athena",
                        "description": description or "",
                        "tables": entries,
                    }
                )
            self._catalog = {"databases": sort_databases(databases)}
        return self._catalog

    def columns(self, dataset_id: str) -> list[str] | None:
        """Return a Glue table's columns (storage columns plus partition keys), cached.

        Args:
            dataset_id: Canonical dataset id; only ``glue://`` ids are looked up.

        Returns:
            Deduplicated column names in Glue's reported order, or ``None`` when
            ``dataset_id`` is not a ``glue`` dataset or Glue has no such table.
        """
        scheme, database, table = split_dataset_id(dataset_id)
        if scheme != "glue":
            return None
        key = (database.lower(), table.lower())
        if key not in self._tables:
            try:
                response = self._call("get_table", DatabaseName=database, Name=table)
                self._remember(database, response["Table"], name=table)
            except _NotFound:
                self._tables[key] = None
        metadata = self._tables[key]
        if metadata is None:
            return None
        return list(dict.fromkeys(c["field_name"] for c in self._columns_of(metadata)))

    def relations(self) -> list[dict]:
        """Glue declares no foreign keys; always empty."""
        return []

    def aliases(self) -> list[tuple[str, str]]:
        """Return ``("glue://db/table", "s3://...")`` pairs for cached tables with a location.

        Returns:
            list[tuple[str, str]]: Sorted pairs; ``s3a``/``s3n`` locations are normalized
            to ``s3``. Only tables already fetched by :meth:`catalog` or :meth:`columns`
            are known.
        """
        pairs = set()
        for (database, table), metadata in self._tables.items():
            location = (metadata or {}).get("StorageDescriptor", {}).get("Location")
            if location and location.lower().startswith(("s3://", "s3a://", "s3n://")):
                pairs.add((f"glue://{database}/{table}", normalize_dataset_id(location)))
        return sorted(pairs)


class GlueSchemaProvider(GlueSchemaSource):
    """Backwards-compatible name and signature of the original Glue lookup."""

    def __init__(self, client=None, *, region: str | None = None):
        """Create a Glue-backed schema provider.

        Args:
            client: A boto3 Glue client (or a compatible fake); built lazily from the
                default session when ``None``.
            region: AWS region for the default client. Ignored when ``client`` is given.
        """
        super().__init__(client=client, region=region)
