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


def test_glue_schema_provider_alias_keeps_old_signature(glue_client):
    from etl_parser.workers import sql

    assert sql.GlueSchemaProvider is GlueSchemaProvider
    provider = GlueSchemaProvider(glue_client)
    assert provider.client is glue_client
    assert provider.columns("glue://raw/events") == ["id"]
    assert isinstance(provider, GlueSchemaSource)
