"""Tests for the agent catalog exporter (review findings 1, 2 and the WP-A additions)."""

import json
from pathlib import Path

import pytest

from etl_parser.export import agent_catalog
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.models import (
    ColumnRef,
    DatasetRef,
    Job,
    JoinCondition,
    LineageDocument,
    Provenance,
    Unresolved,
)
from etl_parser.pipeline import scan
from etl_parser.workers.sql import DictSchemaProvider

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def prior():
    return json.loads((FIXTURES / "etl_catalog_original.json").read_text())


@pytest.fixture(scope="module")
def fixture_document(prior):
    return scan(FIXTURES / "etl", schema=DictSchemaProvider(prior)).document


def column_entries(catalog):
    """Return every ``(db, table, field, key, value)`` tuple under ``databases``."""
    return {
        (d["db_name"], t["table_name"], c["field_name"], key, json.dumps(value, sort_keys=True))
        for d in catalog["databases"]
        for t in d["tables"]
        for c in t["schema"]
        for key, value in c.items()
    }


def test_real_prior_catalog_merges_into_its_sqlite_databases(prior, fixture_document):
    catalog = export_agent_catalog(fixture_document, prior)
    assert [d["db_name"] for d in catalog["databases"]] == ["ecommerce", "analytics_warehouse"]
    assert {d["db_type"] for d in catalog["databases"]} == {"sqlite"}
    before = column_entries(prior)
    assert len(before) == 1373
    assert sum(len(t["schema"]) for d in prior["databases"] for t in d["tables"]) == 321
    assert before <= column_entries(catalog)
    assert catalog["relations"] == prior["relations"]
    tables = {t["table_name"]: t for d in catalog["databases"] for t in d["tables"]}
    assert tables["raw_orders"]["dataset_id"] == "glue://ecommerce/raw_orders"
    assert tables["dim_customer"]["dataset_id"] == "glue://analytics_warehouse/dim_customer"


def test_prior_database_without_type_becomes_athena_not_glue():
    doc = LineageDocument(
        datasets=[DatasetRef(id="glue://db/t", namespace="glue://db", name="t", columns=["x"])]
    )
    prior = {"databases": [{"db_name": "db", "tables": [{"table_name": "t", "schema": []}]}]}
    catalog = export_agent_catalog(doc, prior)
    assert catalog["databases"][0]["db_type"] == "athena"


def test_several_same_name_prior_databases_prefer_matching_scheme():
    doc = LineageDocument(
        datasets=[DatasetRef(id="glue://db/t", namespace="glue://db", name="t", columns=["x"])]
    )
    prior = {
        "databases": [
            {"db_name": "db", "db_type": "postgres", "tables": [{"table_name": "t", "schema": []}]},
            {"db_name": "db", "db_type": "athena", "tables": [{"table_name": "t", "schema": []}]},
        ]
    }
    catalog = export_agent_catalog(doc, prior)
    assert len(catalog["databases"]) == 2
    assert "dataset_id" not in catalog["databases"][0]["tables"][0]
    assert catalog["databases"][1]["tables"][0]["dataset_id"] == "glue://db/t"


def test_glue_database_names_match_prior_case_insensitively():
    doc = LineageDocument(
        datasets=[DatasetRef(id="glue://db/t", namespace="glue://db", name="t", columns=["x"])]
    )
    prior = {"databases": [{"db_name": "DB", "db_type": "sqlite", "tables": [{"table_name": "t"}]}]}
    catalog = export_agent_catalog(doc, prior)
    assert len(catalog["databases"]) == 1
    assert catalog["databases"][0]["tables"][0]["dataset_id"] == "glue://db/t"


def test_prior_scripts_match_by_path_suffix_and_keep_descriptions(prior, fixture_document):
    catalog = export_agent_catalog(fixture_document, prior)
    expected = {s["script_name"]: s["description"] for s in prior["scripts"]}
    actual = {s["script_name"]: s["description"] for s in catalog["scripts"]}
    assert actual == expected
    assert all(s["script_path"] == f"{s['script_name']}.py" for s in catalog["scripts"])


def script_document():
    return LineageDocument(
        jobs=[
            Job(id="alpha", name="alpha", source_file="jobs/alpha.py", language="python"),
            Job(id="beta", name="beta_job", source_file="jobs/beta.py", language="python"),
            Job(id="gamma", name="gamma", source_file="deep/gamma.sql", language="sql"),
            Job(id="delta", name="delta", source_file="delta.sql", language="sql"),
        ]
    )


def test_prior_scripts_match_in_priority_order_and_unmatched_are_kept_last():
    prior = {
        "scripts": [
            {"job_id": "alpha", "script_name": "a", "script_path": "x/a.py", "description": "id"},
            {"script_name": "old_beta", "script_path": "jobs/beta.py", "description": "exact"},
            {"script_name": "gamma", "script_path": "elsewhere/g.sql", "description": "name"},
            {"script_name": "d", "script_path": "repo/delta.sql", "description": "suffix"},
            {"script_name": "legacy", "script_path": "legacy.py", "description": "kept"},
        ]
    }
    catalog = export_agent_catalog(script_document(), prior)
    descriptions = [(s["script_name"], s.get("description")) for s in catalog["scripts"]]
    assert descriptions == [
        ("alpha", "id"),
        ("beta_job", "exact"),
        ("delta", "suffix"),
        ("gamma", "name"),
        ("legacy", "kept"),
    ]
    assert catalog["scripts"][-1] == prior["scripts"][-1]
    assert [s["job_id"] for s in catalog["scripts"][:4]] == ["alpha", "beta", "delta", "gamma"]


def test_prior_descriptions_are_marked_human_and_empty_ones_unmarked(prior, fixture_document):
    catalog = export_agent_catalog(fixture_document, prior)
    assert all(s["description_source"] == "human" for s in catalog["scripts"])
    for database in catalog["databases"]:
        assert database["description_source"] == "human"
        for table in database["tables"]:
            assert (table.get("description_source") == "human") is bool(table["description"])
            for column in table["schema"]:
                has_text = bool(column.get("description"))
                assert (column.get("description_source") == "human") is has_text


def test_script_description_falls_back_to_docstring_first_line(fixture_document):
    catalog = export_agent_catalog(fixture_document)
    jobs = {j.id: j for j in fixture_document.jobs}
    assert len(catalog["scripts"]) == 28
    for script in catalog["scripts"]:
        assert script["description"] == jobs[script["job_id"]].description
        assert script["description_source"] == "code"


def test_code_sourced_script_description_refreshes_but_human_text_is_kept():
    doc = LineageDocument(
        jobs=[
            Job(id="a", name="a", source_file="a.py", description="New docstring."),
            Job(id="b", name="b", source_file="b.py", description="Docstring."),
            Job(id="c", name="c", source_file="c.py"),
        ]
    )
    prior = {
        "scripts": [
            {
                "job_id": "a",
                "script_path": "a.py",
                "description": "Old",
                "description_source": "code",
            },
            {"job_id": "b", "script_path": "b.py", "description": "Analyst text"},
            {"job_id": "c", "script_path": "c.py", "description": ""},
        ]
    }
    scripts = {s["job_id"]: s for s in export_agent_catalog(doc, prior)["scripts"]}
    assert scripts["a"]["description"] == "New docstring."
    assert scripts["a"]["description_source"] == "code"
    assert scripts["b"]["description"] == "Analyst text"
    assert scripts["b"]["description_source"] == "human"
    assert scripts["c"]["description"] == "" and "description_source" not in scripts["c"]


def test_ai_description_keys_on_prior_columns_are_preserved():
    doc = LineageDocument(
        datasets=[DatasetRef(id="glue://db/t", namespace="glue://db", name="t", columns=["x"])]
    )
    column = {
        "field_name": "x",
        "description": "Model text",
        "description_source": "ai",
        "ai_confidence": 0.8,
        "ai_rationale": "Derived from the expression.",
        "ai_model": "example-model",
    }
    prior = {
        "databases": [
            {
                "db_name": "db",
                "db_type": "athena",
                "description": "",
                "tables": [{"table_name": "t", "description": "", "schema": [dict(column)]}],
            }
        ]
    }
    exported = export_agent_catalog(doc, prior)["databases"][0]["tables"][0]["schema"][0]
    assert exported == column
    assert "description_source" not in export_agent_catalog(doc, prior)["databases"][0]


def join(left, right, job_id):
    return JoinCondition(
        left=ColumnRef(dataset_id=left[0], name=left[1]),
        right=ColumnRef(dataset_id=right[0], name=right[1]),
        job_id=job_id,
        provenance=Provenance(parser="sqlglot"),
    )


def test_relations_inferred_from_join_conditions_never_merge_into_relations():
    doc = LineageDocument(
        join_conditions=[
            join(("glue://db/orders", "customer_id"), ("glue://db/customers", "id"), "j2"),
            join(("glue://db/customers", "id"), ("glue://db/orders", "customer_id"), "j1"),
            join(("glue://db/orders", "product_id"), ("glue://db/products", "id"), "j1"),
        ]
    )
    prior = {
        "relations": [
            {
                "from_table": "db.orders",
                "from_column": "customer_id",
                "to_table": "db.customers",
                "to_column": "id",
                "relation_type": "many_to_one",
                "source": "database",
            }
        ]
    }
    catalog = export_agent_catalog(doc, prior)
    assert catalog["relations"] == prior["relations"]
    assert catalog["relations_inferred"] == [
        {
            "from_table": "db.customers",
            "from_column": "id",
            "to_table": "db.orders",
            "to_column": "customer_id",
            "relation_type": "join",
            "source": "inferred",
            "jobs": ["j1", "j2"],
        },
        {
            "from_table": "db.orders",
            "from_column": "product_id",
            "to_table": "db.products",
            "to_column": "id",
            "relation_type": "join",
            "source": "inferred",
            "jobs": ["j1"],
        },
    ]
    empty = export_agent_catalog(LineageDocument())
    assert empty["relations"] == [] and empty["relations_inferred"] == []


def drift_document():
    return LineageDocument(
        datasets=[
            DatasetRef(id="glue://db/t", namespace="glue://db", name="t", columns=["x", "y"]),
            DatasetRef(id="glue://db/new", namespace="glue://db", name="new", columns=["a"]),
            DatasetRef(id="glue://other/o", namespace="glue://other", name="o", columns=["k"]),
        ],
        jobs=[
            Job(id="j1", name="j1", source_file="j1.sql", inputs=["glue://db/new"], outputs=[]),
            Job(id="j2", name="j2", source_file="j2.sql", inputs=[], outputs=["glue://db/t"]),
            Job(id="j3", name="j3", source_file="z/j3.py", inputs=["glue://other/o"], outputs=[]),
        ],
        unresolved=[Unresolved(kind="unknown_column", reason="kept", job_id="j1")],
    )


def drift_prior():
    return {
        "databases": [
            {
                "db_name": "db",
                "db_type": "sqlite",
                "description": "",
                "tables": [
                    {"table_name": "t", "description": "", "schema": [{"field_name": "x"}]},
                    {"table_name": "unused", "description": "", "schema": []},
                ],
            }
        ],
        "relations": [],
    }


def test_code_only_tables_and_columns_are_not_added_by_default():
    catalog = export_agent_catalog(drift_document(), drift_prior())
    assert [d["db_name"] for d in catalog["databases"]] == ["db"]
    table = {t["table_name"]: t for t in catalog["databases"][0]["tables"]}
    assert set(table) == {"t", "unused"}
    assert [c["field_name"] for c in table["t"]["schema"]] == ["x"]
    assert table["t"]["dataset_id"] == "glue://db/t"
    assert catalog["schema_drift"] == {
        "code_only": {
            "databases": [
                {
                    "db_name": "db",
                    "db_type": "athena",
                    "description": "",
                    "tables": [
                        {
                            "table_name": "new",
                            "description": "",
                            "schema": [{"field_name": "a", "datatype": None, "description": ""}],
                            "referenced_by": ["j1"],
                            "source_files": ["j1.sql"],
                        },
                        {
                            "table_name": "t",
                            "description": "",
                            "schema": [{"field_name": "y", "datatype": None, "description": ""}],
                            "referenced_by": ["j2"],
                            "source_files": ["j2.sql"],
                        },
                    ],
                },
                {
                    "db_name": "other",
                    "db_type": "athena",
                    "description": "",
                    "tables": [
                        {
                            "table_name": "o",
                            "description": "",
                            "schema": [{"field_name": "k", "datatype": None, "description": ""}],
                            "referenced_by": ["j3"],
                            "source_files": ["z/j3.py"],
                        }
                    ],
                },
            ]
        },
        "unused_in_code": [{"db_name": "db", "table_name": "unused"}],
    }
    unresolved = catalog["lineage"]["unresolved"]
    assert unresolved[0]["kind"] == "unknown_column"
    missing = [u for u in unresolved if u["kind"] == "missing_in_source"]
    assert [(u["symbols"], u["source_file"]) for u in missing] == [
        (["glue://db/new"], "j1.sql"),
        (["y"], "j2.sql"),
        (["glue://other/o"], "z/j3.py"),
    ]
    assert all(u["job_id"] and u["reason"] and u["remediation"] for u in missing)
    assert all(Unresolved(**u) for u in missing)


def test_include_code_schema_adds_tables_marked_from_code():
    catalog = export_agent_catalog(drift_document(), drift_prior(), include_code_schema=True)
    databases = {d["db_name"]: d for d in catalog["databases"]}
    assert databases["other"]["db_type"] == "athena"
    assert databases["other"]["schema_source"] == "code"
    tables = {t["table_name"]: t for t in databases["db"]["tables"]}
    assert tables["new"]["schema_source"] == "code" and "schema_source" not in tables["t"]
    columns = {c["field_name"]: c for c in tables["t"]["schema"]}
    assert columns["y"] == {"field_name": "y", "description": "", "schema_source": "code"}
    assert "schema_source" not in columns["x"]
    assert catalog["schema_drift"]["unused_in_code"] == [{"db_name": "db", "table_name": "unused"}]
    assert len(catalog["schema_drift"]["code_only"]["databases"]) == 2
    assert export_agent_catalog(drift_document())["databases"] == []


def test_schema_drift_is_logged_once_with_counts(monkeypatch):
    events = []

    class Observer:
        def event(self, name, **fields):
            events.append((name, fields))

    monkeypatch.setattr(agent_catalog, "current_observer", lambda: Observer())
    export_agent_catalog(drift_document(), drift_prior())
    assert events == [
        (
            "schema.drift",
            {
                "actor": "exporter",
                "code_only_tables": 3,
                "code_only_columns": 3,
                "unused_in_code": 1,
            },
        )
    ]
