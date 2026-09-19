"""Tests for the source-of-truth schema fetchers (WP-G). Every transport is mocked."""

from __future__ import annotations

import json

import pytest

from etl_parser.schema.base import SchemaSource, SchemaSourceError
from etl_parser.schema.glue import GlueSchemaProvider, GlueSchemaSource

SECRET = "PRIVATE_TEST_PASSWORD_VALUE"


class FakeClientError(Exception):
    """Mimics a botocore ClientError: carries a ``response`` with an error code."""

    def __init__(self, code, message="raw-provider-text"):
        super().__init__(message)
        self.response = {"Error": {"Code": code, "Message": message}}


def glue_table(name, columns, partitions=(), location=None, description=""):
    table = {
        "Name": name,
        "Description": description,
        "StorageDescriptor": {
            "Columns": [{"Name": c, "Type": t, "Comment": d} for c, t, d in columns],
        },
        "PartitionKeys": [{"Name": c, "Type": t} for c, t in partitions],
    }
    if location:
        table["StorageDescriptor"]["Location"] = location
    return table


class FakeGlueClient:
    """Paginated in-memory Glue Data Catalog with one item per page."""

    def __init__(self, databases, deny=()):
        self.databases = databases
        self.deny = set(deny)
        self.calls = []

    def _page(self, items, token):
        start = int(token or 0)
        page = items[start : start + 1]
        response = {}
        if start + 1 < len(items):
            response["NextToken"] = str(start + 1)
        return page, response

    def get_databases(self, **kwargs):
        self.calls.append("get_databases")
        if "get_databases" in self.deny:
            raise FakeClientError("AccessDeniedException")
        names = sorted(self.databases, reverse=True)  # unsorted on purpose
        page, response = self._page(names, kwargs.get("NextToken"))
        response["DatabaseList"] = [
            {"Name": n, "Description": self.databases[n].get("description", "")} for n in page
        ]
        return response

    def get_database(self, Name):
        self.calls.append("get_database")
        if Name not in self.databases:
            raise FakeClientError("EntityNotFoundException")
        return {"Database": {"Name": Name, "Description": self.databases[Name]["description"]}}

    def get_tables(self, DatabaseName, **kwargs):
        self.calls.append("get_tables")
        if "get_tables" in self.deny:
            raise FakeClientError("AccessDeniedException")
        if DatabaseName not in self.databases:
            raise FakeClientError("EntityNotFoundException")
        tables = self.databases[DatabaseName]["tables"]
        page, response = self._page(tables, kwargs.get("NextToken"))
        response["TableList"] = page
        return response

    def get_table(self, DatabaseName, Name):
        self.calls.append("get_table")
        for table in self.databases.get(DatabaseName, {}).get("tables", []):
            if table["Name"] == Name:
                return {"Table": table}
        raise FakeClientError("EntityNotFoundException")


@pytest.fixture
def glue_client():
    return FakeGlueClient(
        {
            "sales": {
                "description": "Sales mart",
                "tables": [
                    glue_table(
                        "orders",
                        [("order_id", "bigint", "PK"), ("amount", "double", "")],
                        partitions=[("dt", "string")],
                        location="s3a://bucket/sales/orders/",
                        description="Orders",
                    ),
                    glue_table("customers", [("customer_id", "bigint", "")]),
                ],
            },
            "raw": {"description": "", "tables": [glue_table("events", [("id", "string", "")])]},
        }
    )


# ---------------------------------------------------------------- Glue


def test_glue_catalog_shape_pagination_partitions_and_ordering(glue_client):
    source = GlueSchemaSource(client=glue_client)
    assert isinstance(source, SchemaSource)
    catalog = source.catalog()
    assert list(catalog) == ["databases"]
    assert [d["db_name"] for d in catalog["databases"]] == ["raw", "sales"]
    sales = catalog["databases"][1]
    assert sales["db_type"] == "athena"
    assert sales["description"] == "Sales mart"
    assert [t["table_name"] for t in sales["tables"]] == ["customers", "orders"]
    orders = sales["tables"][1]
    assert orders["description"] == "Orders"
    assert orders["location"] == "s3a://bucket/sales/orders/"
    assert orders["schema"] == [
        {"field_name": "order_id", "datatype": "bigint", "description": "PK"},
        {"field_name": "amount", "datatype": "double", "description": ""},
        {"field_name": "dt", "datatype": "string", "description": "", "is_partition": True},
    ]
    assert "location" not in sales["tables"][0]
    assert glue_client.calls.count("get_databases") == 2  # two pages
    assert glue_client.calls.count("get_tables") == 3  # 2 + 1 pages
    assert source.relations() == []
    assert source.aliases() == [("glue://sales/orders", "s3://bucket/sales/orders/")]
    # Second call is served from the cache and is byte-identical.
    assert json.dumps(source.catalog(), sort_keys=True) == json.dumps(catalog, sort_keys=True)
    assert glue_client.calls.count("get_databases") == 2


def test_glue_database_filter_uses_get_database_like_reference_script(glue_client):
    source = GlueSchemaSource(client=glue_client, databases=["sales"])
    catalog = source.catalog()
    assert [d["db_name"] for d in catalog["databases"]] == ["sales"]
    assert catalog["databases"][0]["description"] == "Sales mart"
    assert "get_databases" not in glue_client.calls
    assert glue_client.calls.count("get_database") == 1


def test_glue_columns_shares_cache_with_catalog(glue_client):
    source = GlueSchemaSource(client=glue_client)
    assert source.columns("glue://sales/orders") == ["order_id", "amount", "dt"]
    assert glue_client.calls.count("get_table") == 1
    assert source.columns("glue://sales/orders") == ["order_id", "amount", "dt"]
    assert glue_client.calls.count("get_table") == 1
    assert source.columns("glue://sales/nope") is None
    assert source.columns("postgres://sales/orders") is None
    # Aliases are known for every cached table with an S3 location.
    assert source.aliases() == [("glue://sales/orders", "s3://bucket/sales/orders/")]
    source.catalog()
    assert glue_client.calls.count("get_table") == 2  # only the miss was retried
    assert source.columns("glue://raw/events") == ["id"]
    assert glue_client.calls.count("get_table") == 2  # served from catalog cache


def test_glue_access_denied_names_permission_without_secrets(glue_client):
    glue_client.deny.add("get_databases")
    with pytest.raises(SchemaSourceError, match="glue:GetDatabases") as info:
        GlueSchemaSource(client=glue_client).catalog()
    assert "raw-provider-text" not in str(info.value)  # raw provider message is not echoed
    glue_client.deny = {"get_tables"}
    with pytest.raises(SchemaSourceError, match="glue:GetTables"):
        GlueSchemaSource(client=glue_client, databases=["sales"]).catalog()
    with pytest.raises(SchemaSourceError, match="'missing' not found"):
        GlueSchemaSource(client=glue_client, databases=["missing"]).catalog()


def test_glue_session_and_client_are_built_lazily(monkeypatch):
    import sys
    import types

    built = {}

    class Session:
        def __init__(self, **kwargs):
            built["session"] = kwargs

        def client(self, name):
            built["client"] = name
            return FakeGlueClient({})

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(Session=Session))
    source = GlueSchemaSource(profile="example-profile", region="eu-west-1")
    assert built == {}  # nothing imported or built at construction
    assert source.catalog() == {"databases": []}
    assert built == {
        "session": {"profile_name": "example-profile", "region_name": "eu-west-1"},
        "client": "glue",
    }


class StaticSource:
    """In-memory SchemaSource used to test merging and the SDK surface."""

    def __init__(self, databases, relations=(), aliases=(), columns=None):
        self._databases = databases
        self._relations = list(relations)
        self._aliases = list(aliases)
        self._columns = columns or {}
        self.calls = 0

    def columns(self, dataset_id):
        return self._columns.get(dataset_id)

    def catalog(self):
        self.calls += 1
        return {"databases": json.loads(json.dumps(self._databases))}

    def relations(self):
        return list(self._relations)

    def aliases(self):
        return list(self._aliases)


def table(name, *columns, description=""):
    return {
        "table_name": name,
        "description": description,
        "schema": [{"field_name": c, "datatype": "text", "description": ""} for c in columns],
    }


def relation(parent, child, column, kind="one_to_many", **extra):
    return {
        "from_table": parent,
        "to_table": child,
        "from_column": column,
        "to_column": column,
        "relation_type": kind,
        **extra,
    }


# ---------------------------------------------------------------- catalog merge


def test_write_schema_catalog_merges_sorts_and_is_readable_by_dict_provider(tmp_path):
    from etl_parser.schema import write_schema_catalog
    from etl_parser.workers.sql import DictSchemaProvider

    first = StaticSource(
        [
            {
                "db_name": "z",
                "db_type": "postgresql",
                "description": "",
                "tables": [table("b", "x")],
            },
            {"db_name": "a", "db_type": "athena", "description": "", "tables": [table("t2", "c")]},
        ],
        relations=[relation("z.a", "z.b", "x")],
    )
    second = StaticSource(
        [
            {
                "db_name": "z",
                "db_type": "postgresql",
                "description": "described",
                "tables": [table("a", "x", "y"), table("b", "IGNORED_DUPLICATE")],
            }
        ],
        relations=[relation("z.a", "z.b", "x"), relation("a.t2", "z.b", "c", "one_to_one")],
    )
    out = tmp_path / "nested" / "catalog.json"
    catalog = write_schema_catalog([first, second], out)
    assert list(catalog) == ["databases", "relations"]
    assert [(d["db_name"], d["db_type"]) for d in catalog["databases"]] == [
        ("a", "athena"),
        ("z", "postgresql"),
    ]
    merged = catalog["databases"][1]
    assert merged["description"] == "described"
    assert [t["table_name"] for t in merged["tables"]] == ["a", "b"]
    assert merged["tables"][1]["schema"][0]["field_name"] == "x"  # first source wins
    assert catalog["relations"] == [
        relation("a.t2", "z.b", "c", "one_to_one", source="database"),
        relation("z.a", "z.b", "x", source="database"),
    ]
    assert json.loads(out.read_text()) == catalog
    assert out.read_text().endswith("}\n")
    assert first.calls == 1
    provider = DictSchemaProvider(out)
    assert provider.columns("postgres://z/a") == ["x", "y"]
    assert provider.columns("glue://a/t2") == ["c"]
    # Byte-identical on a second run.
    assert write_schema_catalog([first, second]) == catalog


# ---------------------------------------------------------------- Postgres / Redshift fakes


class FakeCursor:
    def __init__(self, connection):
        self._connection = connection
        self._rows = []

    def execute(self, sql, params=None):
        self._connection.executed.append((sql, params))
        for needle, rows in self._connection.rows:
            if needle in sql:
                self._rows = list(rows)
                return
        raise AssertionError(f"Unexpected SQL: {sql[:80]}")

    def fetchall(self):
        return self._rows


class FakeConnection:
    """DB-API connection whose ``rows`` map a SQL substring to canned rows (first match)."""

    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def cursor(self):
        return FakeCursor(self)


PG_ROWS = [
    (
        "referential_constraints",
        [
            (
                "public",
                "orders_customer_fk",
                "public",
                "orders",
                "customer_id",
                "public",
                "customers",
                "id",
                1,
            ),
            (
                "public",
                "profile_customer_fk",
                "public",
                "profiles",
                "customer_id",
                "public",
                "customers",
                "id",
                1,
            ),
            ("public", "lines_fk", "public", "lines", "order_id", "public", "orders", "id", 1),
            ("public", "lines_fk", "public", "lines", "order_no", "public", "orders", "no", 2),
        ],
    ),
    (
        "table_constraints",
        [
            ("public", "customers_pkey", "PRIMARY KEY", "public", "customers", "id", 1),
            ("public", "profiles_customer_key", "UNIQUE", "public", "profiles", "customer_id", 1),
            ("public", "orders_pkey", "PRIMARY KEY", "public", "orders", "id", 1),
        ],
    ),
    ("pg_attribute", [("public", "customers", "id", "Customer key")]),
    ("objsubid = 0", [("public", "customers", "Customer master")]),
    (
        "information_schema.columns",
        [
            ("public", "orders", "id", "bigint", 1),
            ("public", "orders", "no", "text", 2),
            ("public", "orders", "customer_id", "bigint", 3),
            ("public", "customers", "id", "bigint", 1),
            ("public", "customers", "email", "text", 2),
            ("public", "profiles", "customer_id", "bigint", 1),
            ("public", "lines", "order_id", "bigint", 1),
            ("public", "lines", "order_no", "text", 2),
            ("audit", "log", "id", "integer", 1),
        ],
    ),
]


def test_postgres_catalog_relations_and_columns():
    from etl_parser.schema.postgres import PostgresSchemaSource

    connection = FakeConnection(PG_ROWS)
    source = PostgresSchemaSource(connection=connection, schemas=["public", "audit"])
    catalog = source.catalog()
    assert [d["db_name"] for d in catalog["databases"]] == ["audit", "public"]
    public = catalog["databases"][1]
    assert public["db_type"] == "postgresql"
    assert [t["table_name"] for t in public["tables"]] == [
        "customers",
        "lines",
        "orders",
        "profiles",
    ]
    customers = public["tables"][0]
    assert customers["description"] == "Customer master"
    assert customers["schema"] == [
        {"field_name": "id", "datatype": "bigint", "description": "Customer key"},
        {"field_name": "email", "datatype": "text", "description": ""},
    ]
    assert source.relations() == [
        relation("public.customers", "public.orders", "customer_id", source="database")
        | {"from_column": "id"},
        relation(
            "public.customers", "public.profiles", "customer_id", "one_to_one", source="database"
        )
        | {"from_column": "id"},
        relation("public.orders", "public.lines", "order_id", source="database")
        | {"from_column": "id"},
        relation("public.orders", "public.lines", "order_no", source="database")
        | {"from_column": "no"},
    ]
    assert source.columns("postgres://public/orders") == ["id", "no", "customer_id"]
    assert source.columns("postgres://PUBLIC/Orders") == ["id", "no", "customer_id"]
    assert source.columns("postgres://public/nope") is None
    assert source.columns("glue://public/orders") is None
    assert source.aliases() == []
    # Every query bound the schema filter as a parameter, and ran exactly once.
    assert len(connection.executed) == 5
    assert all(params == {"schemas": ["public", "audit"]} for _sql, params in connection.executed)
    source.catalog()
    assert len(connection.executed) == 5


def test_postgres_never_leaks_dsn(monkeypatch):
    import sys
    import types

    from etl_parser.schema.postgres import PostgresSchemaSource

    dsn = f"postgresql://user:{SECRET}@db.example.internal:5432/app"
    attempts = []

    def connect(*args, **kwargs):
        attempts.append((args, kwargs))
        raise OSError(f"cannot reach {SECRET}")

    monkeypatch.setitem(sys.modules, "psycopg", types.SimpleNamespace(connect=connect))
    source = PostgresSchemaSource(dsn, schemas=["public"])
    assert SECRET not in repr(source) and SECRET not in str(vars(source))
    with pytest.raises(SchemaSourceError) as info:
        source.catalog()
    assert SECRET not in str(info.value) and "OSError" in str(info.value)
    assert attempts == [((dsn,), {})]
    # Environment fallbacks: full DSN first, then PG* variables as keyword arguments.
    monkeypatch.setenv("ETL_PARSER_POSTGRES_DSN", dsn)
    with pytest.raises(SchemaSourceError):
        PostgresSchemaSource().catalog()
    assert attempts[-1] == ((dsn,), {})
    monkeypatch.delenv("ETL_PARSER_POSTGRES_DSN")
    monkeypatch.setenv("PGHOST", "db.example.internal")
    monkeypatch.setenv("PGDATABASE", "app")
    monkeypatch.setenv("PGUSER", "user")
    monkeypatch.setenv("PGPASSWORD", SECRET)
    with pytest.raises(SchemaSourceError):
        PostgresSchemaSource().catalog()
    assert attempts[-1] == (
        (),
        {"host": "db.example.internal", "dbname": "app", "user": "user", "password": SECRET},
    )
    for name in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        monkeypatch.delenv(name)
    with pytest.raises(SchemaSourceError, match="ETL_PARSER_POSTGRES_DSN"):
        PostgresSchemaSource().catalog()


def test_no_credentials_reach_any_serialized_output(tmp_path, monkeypatch, caplog):
    import logging

    from etl_parser.schema import write_schema_catalog
    from etl_parser.schema.postgres import PostgresSchemaSource
    from etl_parser.schema.redshift import RedshiftSchemaSource

    monkeypatch.setenv("PGPASSWORD", SECRET)
    monkeypatch.setenv("REDSHIFT_PASSWORD", SECRET)
    sources = [
        PostgresSchemaSource(
            f"postgresql://user:{SECRET}@db.example.internal/app",
            connection=FakeConnection(PG_ROWS),
        ),
        RedshiftSchemaSource(
            f"redshift://user:{SECRET}@cluster.example.internal/dw",
            connection=FakeConnection(RS_ROWS),
        ),
    ]
    out = tmp_path / "catalog.json"
    with caplog.at_level(logging.DEBUG):
        catalog = write_schema_catalog(sources, out)
    for source in sources:
        assert SECRET not in json.dumps(source.catalog())
        assert SECRET not in json.dumps(source.relations())
        assert SECRET not in repr(source) and SECRET not in str(vars(source))
    assert SECRET not in json.dumps(catalog)
    assert SECRET not in out.read_text()
    assert SECRET not in caplog.text


def test_postgres_missing_driver_names_extra(monkeypatch):
    import builtins

    from etl_parser.schema.postgres import PostgresSchemaSource

    original = builtins.__import__

    def no_driver(name, *args, **kwargs):
        if name == "psycopg":
            raise ImportError("No module named 'psycopg'")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_driver)
    with pytest.raises(ImportError, match=r"etl-parser\[postgres\]"):
        PostgresSchemaSource("postgresql://localhost/app").catalog()


RS_ROWS = [
    (
        "svv_columns",
        [
            ("dw", "orders", "id", "bigint", 1),
            ("dw", "orders", "customer_id", "bigint", 2),
            ("dw", "customers", "id", "bigint", 1),
            ("dw", "profiles", "customer_id", "bigint", 1),
        ],
    ),
    ("svv_table_info", [("dw", "orders"), ("dw", "customers"), ("dw", "empty_table")]),
    ("pg_attribute", [("dw", "orders", "id", "Order key")]),
    ("objsubid = 0", [("dw", "orders", "Orders fact")]),
    (
        "pg_constraint",
        [
            ("dw", "customers", "customers_pkey", "p", "PRIMARY KEY (id)"),
            (
                "dw",
                "orders",
                "orders_fk",
                "f",
                'FOREIGN KEY (customer_id) REFERENCES dw.customers("id")',
            ),
            ("dw", "profiles", "profiles_uq", "u", "UNIQUE (customer_id)"),
            (
                "dw",
                "profiles",
                "profiles_fk",
                "f",
                "FOREIGN KEY (customer_id) REFERENCES customers(id)",
            ),
        ],
    ),
]


def test_redshift_catalog_relations_and_inline_schema_filter():
    from etl_parser.schema.redshift import RedshiftSchemaSource

    connection = FakeConnection(RS_ROWS)
    source = RedshiftSchemaSource(connection=connection, schemas=["dw"])
    catalog = source.catalog()
    assert [d["db_name"] for d in catalog["databases"]] == ["dw"]
    dw = catalog["databases"][0]
    assert dw["db_type"] == "redshift"
    assert [t["table_name"] for t in dw["tables"]] == [
        "customers",
        "empty_table",
        "orders",
        "profiles",
    ]
    assert dw["tables"][1]["schema"] == []
    orders = dw["tables"][2]
    assert orders["description"] == "Orders fact"
    assert orders["schema"][0] == {
        "field_name": "id",
        "datatype": "bigint",
        "description": "Order key",
    }
    assert source.relations() == [
        relation("dw.customers", "dw.orders", "customer_id", source="database")
        | {"from_column": "id"},
        relation("dw.customers", "dw.profiles", "customer_id", "one_to_one", source="database")
        | {"from_column": "id"},
    ]
    assert source.columns("redshift://dw/orders") == ["id", "customer_id"]
    assert source.columns("redshift://dw/empty_table") == []
    assert source.columns("postgres://dw/orders") is None
    assert all(params is None for _sql, params in connection.executed)
    assert all("IN ('dw')" in sql for sql, _params in connection.executed)
    assert "%(" not in "".join(sql for sql, _params in connection.executed)
    with pytest.raises(SchemaSourceError, match="identifier"):
        RedshiftSchemaSource(connection=connection, schemas=["dw; drop"])


def test_redshift_iam_and_password_connections(monkeypatch):
    import sys
    import types

    from etl_parser.schema.redshift import RedshiftSchemaSource

    attempts = []

    def connect(**kwargs):
        attempts.append(kwargs)
        return FakeConnection(RS_ROWS)

    monkeypatch.setitem(sys.modules, "redshift_connector", types.SimpleNamespace(connect=connect))
    dsn = f"redshift://etl:{SECRET}@cluster.example.internal:5439/dw"
    RedshiftSchemaSource(dsn).catalog()
    assert attempts[-1] == {
        "host": "cluster.example.internal",
        "port": 5439,
        "database": "dw",
        "user": "etl",
        "password": SECRET,
    }
    RedshiftSchemaSource(
        dsn, iam=True, profile="example-profile", region="eu-west-1", cluster_identifier="c1"
    ).catalog()
    assert attempts[-1] == {
        "host": "cluster.example.internal",
        "port": 5439,
        "database": "dw",
        "user": "etl",
        "iam": True,
        "profile": "example-profile",
        "region": "eu-west-1",
        "cluster_identifier": "c1",
        "db_user": "etl",
    }
    monkeypatch.setenv("ETL_PARSER_REDSHIFT_DSN", dsn)
    monkeypatch.setenv("REDSHIFT_DB_USER", "svc")
    source = RedshiftSchemaSource(iam=True, cluster_identifier="c2")
    source.catalog()
    assert attempts[-1]["db_user"] == "svc" and attempts[-1]["cluster_identifier"] == "c2"
    assert "password" not in attempts[-1]
    assert SECRET not in repr(source) and SECRET not in str(vars(source))
    monkeypatch.delenv("ETL_PARSER_REDSHIFT_DSN")
    monkeypatch.delenv("REDSHIFT_DB_USER")
    with pytest.raises(SchemaSourceError, match="ETL_PARSER_REDSHIFT_DSN") as info:
        RedshiftSchemaSource().catalog()
    assert SECRET not in str(info.value)


def test_redshift_missing_driver_names_extra(monkeypatch):
    import builtins

    from etl_parser.schema.redshift import RedshiftSchemaSource

    original = builtins.__import__

    def no_driver(name, *args, **kwargs):
        if name == "redshift_connector":
            raise ImportError("No module named 'redshift_connector'")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_driver)
    with pytest.raises(ImportError, match=r"etl-parser\[redshift\]"):
        RedshiftSchemaSource("redshift://h/db").catalog()


# ---------------------------------------------------------------- CLI


def summary_line(output, key):
    """Return the command's JSON summary, ignoring interleaved observer log lines."""
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                parsed = json.loads(line)
            except ValueError:
                continue
            if key in parsed:
                return parsed
    raise AssertionError(f"No summary with {key!r} in output: {output}")


def test_schema_fetch_cli_builds_sources_from_flags_and_environment_only(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    import etl_parser.schema as schema_module
    from etl_parser.cli import app

    built = {}

    def factory(name):
        def make(*args, **kwargs):
            built[name] = (args, kwargs)
            return StaticSource(
                [
                    {
                        "db_name": name,
                        "db_type": name,
                        "description": "",
                        "tables": [table("t", "c")],
                    }
                ],
                relations=[relation(f"{name}.t", f"{name}.t", "c")],
            )

        return make

    monkeypatch.setattr(schema_module, "GlueSchemaSource", factory("glue"))
    monkeypatch.setattr(schema_module, "PostgresSchemaSource", factory("postgres"))
    monkeypatch.setattr(schema_module, "RedshiftSchemaSource", factory("redshift"))
    out = tmp_path / "out" / "catalog.json"
    result = CliRunner().invoke(
        app,
        [
            "schema",
            "fetch",
            "--glue",
            "--database",
            "sales",
            "--database",
            "raw",
            "--postgres",
            "--redshift",
            "--iam",
            "--schema-name",
            "public",
            "--aws-profile",
            "example-profile",
            "--region",
            "eu-west-1",
            "--out",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    assert built["glue"] == (
        (),
        {"profile": "example-profile", "region": "eu-west-1", "databases": ["sales", "raw"]},
    )
    assert built["postgres"] == ((), {"schemas": ["public"]})
    assert built["redshift"] == (
        (),
        {"schemas": ["public"], "iam": True, "profile": "example-profile", "region": "eu-west-1"},
    )
    assert summary_line(result.output, "out") == {
        "databases": 3,
        "tables": 3,
        "relations": 3,
        "out": str(out),
    }
    written = json.loads(out.read_text())
    assert [d["db_name"] for d in written["databases"]] == ["glue", "postgres", "redshift"]
    assert all(r["source"] == "database" for r in written["relations"])
    # No source selected, and no way to pass credentials on the command line.
    assert CliRunner().invoke(app, ["schema", "fetch"]).exit_code == 2
    denied = CliRunner().invoke(app, ["schema", "fetch", "--postgres", "--dsn", "x"])
    assert denied.exit_code == 2 and "No such option" in denied.output
    for flag in ("--password", "--dsn"):
        assert flag not in CliRunner().invoke(app, ["schema", "fetch", "--help"]).output


def test_schema_fetch_cli_reports_source_errors_without_secrets(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    import etl_parser.schema as schema_module
    from etl_parser.cli import app

    class Broken:
        def __init__(self, **kwargs):
            pass

        def catalog(self):
            raise SchemaSourceError("Access denied calling get_tables; grant glue:GetTables")

        def relations(self):
            return []

        def aliases(self):
            return []

        def columns(self, dataset_id):
            return None

    monkeypatch.setattr(schema_module, "GlueSchemaSource", Broken)
    monkeypatch.setenv("PGPASSWORD", SECRET)
    result = CliRunner().invoke(
        app, ["schema", "fetch", "--glue", "--out", str(tmp_path / "c.json")]
    )
    assert result.exit_code == 2
    assert "glue:GetTables" in result.output and SECRET not in result.output
    assert not (tmp_path / "c.json").exists()


def test_scan_cli_glue_uses_profile_region_and_schema_from_code(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    import etl_parser.cli as cli_module
    from etl_parser.cli import app

    built = {}

    def glue_source(**kwargs):
        built.update(kwargs)
        return StaticSource([])

    monkeypatch.setattr(cli_module, "GlueSchemaSource", glue_source)
    (tmp_path / "job.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    result = CliRunner().invoke(
        app,
        [
            "scan",
            str(tmp_path),
            "--out",
            str(tmp_path / "lineage.json"),
            "--glue",
            "--aws-profile",
            "example-profile",
            "--region",
            "eu-west-1",
            "--schema-from-code",
        ],
    )
    assert result.exit_code == 0, result.output
    assert built == {"profile": "example-profile", "region": "eu-west-1"}


def test_run_cli_threads_schema_from_code_to_the_exporter(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    import etl_parser.sdk as sdk
    from etl_parser.cli import app

    seen = {}
    original = sdk.apply_export_options

    def record(result, **kwargs):
        seen.update(kwargs)
        return original(result, **kwargs)

    monkeypatch.setattr(sdk, "apply_export_options", record)
    monkeypatch.setattr("etl_parser.cli.apply_export_options", record)
    (tmp_path / "job.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    result = CliRunner().invoke(
        app,
        ["run", str(tmp_path), "--out-dir", str(tmp_path / "artifacts"), "--schema-from-code"],
    )
    assert result.exit_code == 0, result.output
    assert seen["include_code_schema"] is True
    assert seen["prior"] is None and seen["schema"] is None


# ---------------------------------------------------------------- SDK surface


def test_scan_accepts_schema_source_dict_and_path_and_records_export_options(tmp_path):
    import etl_parser
    from etl_parser.workers.sql import DictSchemaProvider

    (tmp_path / "job.sql").write_text("CREATE TABLE db.t AS SELECT * FROM db.s")
    catalog = {
        "databases": [
            {
                "db_name": "db",
                "db_type": "athena",
                "description": "",
                "tables": [table("s", "x", "y")],
            }
        ]
    }
    graph = etl_parser.scan(
        tmp_path, schema=catalog, generate=["scripts", "databases"], databases=["db"]
    )
    assert graph.schema_source is None
    assert graph.export_options == {
        "include_code_schema": False,
        "generate": ["databases", "scripts"],
        "databases": ["db"],
    }
    assert {e.target.name for e in graph.document.column_edges} == {"x", "y"}  # star expanded
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(catalog))
    assert len(etl_parser.scan(tmp_path, schema=path).document.column_edges) == 2
    assert (
        len(etl_parser.scan(tmp_path, schema=DictSchemaProvider(catalog)).document.column_edges)
        == 2
    )
    source = StaticSource(
        catalog["databases"],
        aliases=[("glue://db/s", "s3://bucket/s/"), ("glue://other/unused", "s3://bucket/u/")],
        columns={"glue://db/s": ["x", "y"]},
    )
    graph = etl_parser.scan(tmp_path, schema=source, include_code_schema=True)
    assert graph.schema_source is source
    assert graph.export_options["include_code_schema"] is True
    datasets = {d.id: d for d in graph.document.datasets}
    assert datasets["glue://db/s"].aliases == ["s3://bucket/s/"]
    assert datasets["glue://db/s"].physical_location == "s3://bucket/s/"
    assert "glue://other/unused" not in datasets


def test_client_forwards_export_options_only_when_exporter_supports_them(tmp_path, monkeypatch):
    import etl_parser.sdk as sdk
    from etl_parser import ParserClient

    (tmp_path / "job.sql").write_text("CREATE TABLE db.t AS SELECT x FROM db.s")
    source = StaticSource([])
    client = ParserClient(log_level="ERROR")
    # Current exporter: options are dropped silently, schema_drift is absent -> None.
    result = client.run(tmp_path, schema=source, generate=["databases"], databases=["db"])
    assert result.schema_drift is None
    assert "schema_drift" not in result.catalog
    seen = {}

    def fake_export(
        doc, prior=None, *, schema=None, include_code_schema=False, generate=None, databases=None
    ):
        seen.update(
            schema=schema,
            include_code_schema=include_code_schema,
            generate=generate,
            databases=databases,
            prior=prior,
        )
        return {
            "databases": [],
            "relations": [],
            "schema_drift": {"code_only": {"databases": []}, "unused_in_code": []},
        }

    monkeypatch.setattr(sdk, "export_agent_catalog", fake_export)
    result = client.run(
        tmp_path,
        schema=source,
        include_code_schema=True,
        generate=["databases", "scripts"],
        databases=["db"],
        prior={"databases": [], "relations": [], "marker": 1},
    )
    assert seen == {
        "schema": source,
        "include_code_schema": True,
        "generate": ["databases", "scripts"],
        "databases": ["db"],
        "prior": {"databases": [], "relations": [], "marker": 1},
    }
    assert result.schema_drift == {"code_only": {"databases": []}, "unused_in_code": []}
    assert result.to_dict()["catalog"]["schema_drift"] == result.schema_drift
    assert sdk.apply_export_options(result, generate=["databases"]) == []


def test_client_fetch_schema_and_lazy_package_exports():
    import sys

    import etl_parser
    from etl_parser import ParserClient

    for name in ("psycopg", "redshift_connector"):
        assert name not in sys.modules
    assert etl_parser.GlueSchemaSource is GlueSchemaSource
    assert etl_parser.write_schema_catalog is etl_parser.schema.write_schema_catalog
    assert etl_parser.SchemaSourceError is SchemaSourceError
    with pytest.raises(AttributeError):
        etl_parser.NotARealExport  # noqa: B018
    catalog = ParserClient.fetch_schema(
        [StaticSource([{"db_name": "a", "db_type": "athena", "description": "", "tables": []}])]
    )
    assert catalog == {
        "databases": [{"db_name": "a", "db_type": "athena", "description": "", "tables": []}],
        "relations": [],
    }


def test_glue_schema_provider_alias_keeps_old_signature(glue_client):
    from etl_parser.workers import sql

    assert sql.GlueSchemaProvider is GlueSchemaProvider
    provider = GlueSchemaProvider(glue_client)
    assert provider.client is glue_client
    assert provider.columns("glue://raw/events") == ["id"]
    assert isinstance(provider, GlueSchemaSource)
