from etl_parser.describe.client import StubClient
from etl_parser.describe.engine import DescriptionEngine
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.pipeline import scan


def test_identity_inherits_and_expression_uses_grounded_prompt(tmp_path):
    (tmp_path / "job.sql").write_text(
        "CREATE TABLE b.t AS SELECT x AS renamed, x * 2 AS twice FROM a.s"
    )
    doc = scan(tmp_path).document
    catalog = export_agent_catalog(doc)
    source = next(d for d in catalog["databases"] if d["db_name"] == "a")
    source["tables"][0]["schema"][0]["description"] = "Amount in cents"
    client = StubClient(['{"description":"Double the amount in cents"}'])
    result = DescriptionEngine(client).run(doc, catalog)
    target = next(d for d in result["databases"] if d["db_name"] == "b")
    columns = {c["field_name"]: c for c in target["tables"][0]["schema"]}
    assert columns["renamed"]["description"] == "Amount in cents"
    assert columns["twice"]["description_source"] == "ai"
    assert len(client.calls) == 1 and "Amount in cents" in client.calls[0][1]
    assert "* 2" in client.calls[0][1]


def test_malformed_description_is_skipped_with_warning(tmp_path):
    (tmp_path / "job.sql").write_text("CREATE TABLE b.t AS SELECT x * 2 AS y FROM a.s")
    doc = scan(tmp_path).document
    engine = DescriptionEngine(StubClient(["not JSON"]))
    engine.run(doc, export_agent_catalog(doc))
    assert len(engine.warnings) == 1
