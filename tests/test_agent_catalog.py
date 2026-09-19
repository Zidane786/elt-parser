"""Tests for the agent catalog exporter (review findings 1, 2 and the WP-A additions)."""

import json
from pathlib import Path

import pytest

from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.models import DatasetRef, Job, LineageDocument
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
