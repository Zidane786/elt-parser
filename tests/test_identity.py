from etl_parser.identity import (
    DatasetRegistry,
    agent_table_name,
    dataset_ref_from_id,
    normalize_dataset_id,
    split_dataset_id,
)
from etl_parser.models import DatasetRef


def test_glue_tables_lowercase_for_spark_and_athena():
    assert (
        normalize_dataset_id("Ecommerce.Raw_Orders", engine="spark")
        == "glue://ecommerce/raw_orders"
    )
    assert (
        normalize_dataset_id("orders", engine="athena", default_db="Analytics")
        == "glue://analytics/orders"
    )
    assert normalize_dataset_id("awsdatacatalog.db.t", engine="trino") == "glue://db/t"


def test_uris_pass_through_with_scheme_normalised():
    assert normalize_dataset_id("s3://b/p/", engine="spark") == "s3://b/p/"
    assert normalize_dataset_id("s3a://b/p", engine="spark") == "s3://b/p"
    assert normalize_dataset_id("/data/x.csv", engine="pandas") == "file:///data/x.csv"


def test_postgres_keeps_case_only_when_quoted():
    assert (
        normalize_dataset_id("billing_pg.Invoices", engine="postgres")
        == "postgres://billing_pg/invoices"
    )
    assert (
        normalize_dataset_id('public."MixedCase"', engine="postgresql")
        == "postgres://public/MixedCase"
    )
    assert (
        normalize_dataset_id("t", engine="postgres", default_db="public") == "postgres://public/t"
    )


def test_split_and_agent_name():
    assert split_dataset_id("glue://db/t") == ("glue", "db", "t")
    assert split_dataset_id("s3://bucket/a/b/") == ("s3", "bucket", "a/b/")
    assert agent_table_name("glue://db/t") == "db.t"
    assert agent_table_name("s3://bucket/a/") == "s3://bucket/a/"
    assert dataset_ref_from_id("s3://bucket/a/").kind == "s3_path"
    assert dataset_ref_from_id("glue://db/t").namespace == "glue://db"


def test_registry_merges_aliases_and_columns():
    reg = DatasetRegistry()
    reg.add(DatasetRef(id="glue://db/t", namespace="glue://db", name="t", columns=["a"]))
    reg.add(DatasetRef(id="glue://db/t", namespace="glue://db", name="t", columns=["b"]))
    reg.add(DatasetRef(id="s3://bucket/t/", namespace="s3://bucket", name="t/", kind="s3_path"))
    reg.merge_alias("s3://bucket/t/", "glue://db/t")
    refs = reg.all()
    assert [r.id for r in refs] == ["glue://db/t"]
    assert refs[0].columns == ["a", "b"]
    assert refs[0].aliases == ["s3://bucket/t/"]
    assert refs[0].physical_location == "s3://bucket/t/"
    assert reg.resolve_id("s3://bucket/t/") == "glue://db/t"
    assert reg.get_or_create("s3://bucket/t/").id == "glue://db/t"
