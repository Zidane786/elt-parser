from pathlib import Path

from etl_parser.workers.sql import DictSchemaProvider, SqlWorker

FIXTURES = Path(__file__).parent / "fixtures"


def _edges_by_target(result):
    return {e.target.name: e for e in result.column_edges}


def _src(edge):
    return sorted((s.dataset_id, s.name) for s in edge.sources)


def test_simple_select_into_ctas_identity_and_expression():
    w = SqlWorker()
    a = w.analyze(
        "CREATE TABLE out.t AS SELECT a, b + 1 AS b1, 'x' AS lit FROM src.s",
        dialect="trino",
        engine="athena",
        job_id="j",
    )
    assert a.inputs == {"glue://src/s"} and a.outputs == {"glue://out/t"}
    e = _edges_by_target(a.result)
    assert _src(e["a"]) == [("glue://src/s", "a")] and e["a"].transformation.kind == "identity"
    assert _src(e["b1"]) == [("glue://src/s", "b")] and e["b1"].transformation.kind == "expression"
    assert e["lit"].sources == [] and e["lit"].provenance.confidence == "exact"
    assert a.output_columns["glue://out/t"] == ["a", "b1", "lit"]
    assert [(t.source, t.target) for t in a.result.table_edges] == [
        ("glue://src/s", "glue://out/t")
    ]


def test_reg_ctas_with_join_and_aggregation_from_fixture():
    sql = (FIXTURES / "products/reg/sql/mart_energy_sold.sql").read_text()
    a = SqlWorker().analyze(sql, dialect="trino", engine="athena", job_id="reg")
    assert a.inputs == {"glue://bill_cur/fact_invoice", "glue://meter_cur/fact_consumption"}
    assert a.outputs == {"glue://reg_cur/mart_energy_sold"}
    e = _edges_by_target(a.result)
    assert _src(e["period"]) == [("glue://bill_cur/fact_invoice", "issue_date")]
    assert e["period"].transformation.kind == "expression"
    assert _src(e["total_kwh"]) == [("glue://meter_cur/fact_consumption", "kwh")]
    assert e["total_kwh"].transformation.kind == "aggregation"
    assert "SUM" in e["total_kwh"].transformation.expression.upper()
    join_refs = sorted((r.dataset_id, r.name) for r in e["total_kwh"].indirect_sources)
    assert ("glue://bill_cur/fact_invoice", "account_id") in join_refs
    assert ("glue://meter_cur/fact_consumption", "account_id") in join_refs
    assert e["total_kwh"].transformation.line_start == 13
    assert a.result.unresolved == []


def test_analyze_file_creates_job_with_header_schedule():
    path = FIXTURES / "products/reg/sql/mart_saidi_saifi.sql"
    a = SqlWorker().analyze_file(path, root=FIXTURES / "products/reg")
    job = a.result.jobs[0]
    assert job.id == "sql/mart_saidi_saifi" and job.language == "sql" and job.dialect == "trino"
    assert job.inputs == ["glue://reg_stg/reliability_events"]
    assert job.outputs == ["glue://reg_cur/mart_saidi_saifi"]
    assert job.owner == "regulatory-reporting@utility.example.com"
    assert job.description.startswith("SAIDI/SAIFI reliability indices")
    sched = a.result.schedules[job.schedule_id]
    assert sched.cron == "0 5 1 * *" and sched.orchestrator == "cron_comment"
    e = _edges_by_target(a.result)
    assert e["saidi"].transformation.kind == "aggregation"
    assert _src(e["saidi"]) == [
        ("glue://reg_stg/reliability_events", "customers_affected"),
        ("glue://reg_stg/reliability_events", "minutes"),
    ]


def test_cte_and_insert_select():
    sql = """
    WITH recent AS (
        SELECT o.order_id, o.customer_id, o.amount FROM shop.orders o WHERE o.dt >= '2026-01-01'
    )
    INSERT INTO mart.big_spenders
    SELECT c.customer_id, SUM(r.amount) AS total
    FROM recent r JOIN shop.customers c ON r.customer_id = c.id
    GROUP BY c.customer_id
    """
    a = SqlWorker().analyze(sql, dialect="spark", engine="spark", job_id="j")
    assert a.inputs == {"glue://shop/orders", "glue://shop/customers"}
    e = _edges_by_target(a.result)
    assert _src(e["customer_id"]) == [("glue://shop/customers", "customer_id")]
    assert _src(e["total"]) == [("glue://shop/orders", "amount")]
    assert e["total"].transformation.kind == "aggregation"
    assert e["total"].provenance.confidence == "exact"


def test_star_without_schema_gives_table_edge_and_missing_schema():
    a = SqlWorker().analyze("CREATE TABLE b.t AS SELECT * FROM a.s", dialect="trino", job_id="j")
    assert a.result.column_edges == []
    assert [u.kind for u in a.result.unresolved] == ["missing_schema"]
    assert [(t.source, t.target) for t in a.result.table_edges] == [("glue://a/s", "glue://b/t")]


def test_star_with_schema_expands_columns():
    schema = DictSchemaProvider({"a": {"s": ["x", "y"]}})
    a = SqlWorker(schema).analyze(
        "CREATE TABLE b.t AS SELECT * FROM a.s", dialect="trino", job_id="j"
    )
    e = _edges_by_target(a.result)
    assert set(e) == {"x", "y"} and _src(e["x"]) == [("glue://a/s", "x")]
    assert a.result.unresolved == []


def test_dict_schema_from_agent_catalog():
    p = DictSchemaProvider(FIXTURES / "etl_catalog_original.json")
    assert "order_id" in p.columns("glue://ecommerce/raw_orders")
    assert p.columns("glue://nope/x") is None


def test_unparseable_sql_is_unresolved_not_exception():
    a = SqlWorker().analyze("SELEC broken FROM", dialect="trino", job_id="j")
    assert a.result.column_edges == []
    assert a.result.unresolved[0].kind == "unsupported_syntax"


def test_merge_update_and_insert_branches():
    sql = """
    MERGE INTO dw.dim_customer t USING stg.customers s ON t.customer_id = s.customer_id
    WHEN MATCHED THEN UPDATE SET t.tier = s.tier, t.updated_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (customer_id, tier, country)
    VALUES (s.customer_id, s.tier, upper(s.country))
    """
    a = SqlWorker().analyze(sql, dialect="spark", engine="spark", job_id="j")
    assert a.outputs == {"glue://dw/dim_customer"}
    assert "glue://stg/customers" in a.inputs
    e = _edges_by_target(a.result)
    assert _src(e["tier"]) == [("glue://stg/customers", "tier")]
    assert _src(e["country"]) == [("glue://stg/customers", "country")]
    assert e["country"].transformation.kind == "expression"
    assert e["updated_at"].sources == []
    assert ("glue://stg/customers", "customer_id") in [
        (r.dataset_id, r.name) for r in e["tier"].indirect_sources
    ]


def test_temp_table_is_collapsed_across_statements():
    sql = """
    CREATE TEMPORARY TABLE tmp_orders AS SELECT order_id, amount * 100 AS cents FROM shop.orders;
    CREATE TABLE mart.cents AS SELECT order_id, cents FROM tmp_orders;
    """
    a = SqlWorker().analyze(sql, dialect="spark", engine="spark", job_id="j", default_db="default")
    e = {(x.target.dataset_id, x.target.name): x for x in a.result.column_edges}
    final = e[("glue://mart/cents", "cents")]
    assert _src(final) == [("glue://shop/orders", "amount")]
    assert final.transformation.line_start == 3


def test_plain_select_has_inputs_and_output_columns_only():
    a = SqlWorker().analyze(
        "SELECT invoice_id, amount FROM billing_pg.invoices",
        dialect="postgres",
        engine="postgres",
        job_id="j",
    )
    assert a.inputs == {"postgres://billing_pg/invoices"} and a.outputs == set()
    assert a.output_columns["__select__"] == ["invoice_id", "amount"]
    assert a.result.column_edges == []


def test_select_with_target_override_tracks_frame():
    a = SqlWorker().analyze(
        "SELECT invoice_id, amount FROM billing_pg.invoices",
        dialect="postgres",
        engine="postgres",
        job_id="j",
        target_override="frame://j/invoices",
    )
    e = _edges_by_target(a.result)
    assert _src(e["amount"]) == [("postgres://billing_pg/invoices", "amount")]


def test_parse_failure_preserves_other_statements_and_locations():
    a = SqlWorker().analyze(
        "CREATE TABLE b.first AS SELECT x FROM a.s;\n"
        "SELECT FROM;\n"
        "CREATE TABLE b.last AS SELECT y FROM a.t;",
        line_offset=10,
    )
    assert a.outputs == {"glue://b/first", "glue://b/last"}
    assert a.result.unresolved[0].line == 12
    assert a.result.column_edges[-1].transformation.line_start == 13


def test_semicolons_inside_strings_and_comments_are_not_boundaries():
    a = SqlWorker().analyze(
        "-- ignored ;\nCREATE TABLE b.t AS SELECT ';' AS x FROM a.s;\n;\n"
        "CREATE TABLE b.u AS SELECT y FROM a.t;"
    )
    assert a.result.unresolved == []
    assert [e.transformation.line_start for e in a.result.column_edges] == [2, 4]


def test_temp_tables_do_not_leak_between_calls():
    worker = SqlWorker()
    worker.analyze("CREATE TEMP TABLE tmp AS SELECT x FROM a.s", dialect="postgres")
    a = worker.analyze("CREATE TABLE b.t AS SELECT x FROM tmp", dialect="postgres")
    assert _src(a.result.column_edges[0]) == [("glue://default/tmp", "x")]


def test_temp_star_and_indirect_sources_are_expanded():
    a = SqlWorker().analyze(
        "CREATE TEMP TABLE tmp AS SELECT x FROM a.s WHERE active = 1;"
        "CREATE TABLE b.t AS SELECT * FROM tmp;",
        dialect="postgres",
    )
    edge = a.result.column_edges[-1]
    assert edge.target.dataset_id == "glue://b/t"
    assert _src(edge) == [("glue://a/s", "x")]
    assert [(r.dataset_id, r.name) for r in edge.indirect_sources] == [("glue://a/s", "active")]
    assert a.result.unresolved == []


def test_insert_target_columns_follow_position():
    a = SqlWorker().analyze("INSERT INTO b.t (second, first) SELECT x, y FROM a.s")
    edges = _edges_by_target(a.result)
    assert a.output_columns["glue://b/t"] == ["second", "first"]
    assert _src(edges["second"]) == [("glue://a/s", "x")]
    assert _src(edges["first"]) == [("glue://a/s", "y")]


def test_insert_column_count_mismatch_is_reported():
    a = SqlWorker().analyze("INSERT INTO b.t (x, y) SELECT x FROM a.s")
    assert a.result.unresolved
    assert not a.result.column_edges


def test_nested_cte_predicates_resolve_to_physical_columns():
    a = SqlWorker().analyze(
        "CREATE TABLE b.t AS WITH c AS (SELECT x, flag FROM a.s WHERE dt > 0) "
        "SELECT x FROM c WHERE flag = 1"
    )
    assert a.inputs == {"glue://a/s"}
    edge = a.result.column_edges[0]
    assert {(r.dataset_id, r.name) for r in edge.indirect_sources} == {
        ("glue://a/s", "dt"),
        ("glue://a/s", "flag"),
    }


def test_cte_name_does_not_hide_physical_table_in_other_scope():
    a = SqlWorker().analyze(
        "CREATE TABLE b.t AS SELECT x FROM real_table UNION ALL "
        "SELECT x FROM (WITH real_table AS (SELECT x FROM a.s) "
        "SELECT x FROM real_table) q"
    )
    assert a.inputs == {"glue://default/real_table", "glue://a/s"}


def test_partially_known_star_schema_is_reported():
    worker = SqlWorker(DictSchemaProvider({"a": {"s": ["x"]}}))
    a = worker.analyze("CREATE TABLE b.t AS SELECT * FROM a.s JOIN a.u ON s.x = u.x")
    assert any(u.kind == "missing_schema" for u in a.result.unresolved)
    assert all(e.target.name != "*" for e in a.result.column_edges)


def test_use_updates_default_database():
    a = SqlWorker().analyze("USE analytics; CREATE TABLE target AS SELECT x FROM source")
    assert a.inputs == {"glue://analytics/source"}
    assert a.outputs == {"glue://analytics/target"}


def test_quoted_postgres_table_names_preserve_case():
    a = SqlWorker().analyze(
        'CREATE TABLE "Out"."Dest" AS SELECT "Value" FROM "Src"."Data"',
        dialect="postgres",
        engine="postgres",
    )
    assert a.inputs == {"postgres://Src/Data"}
    assert a.outputs == {"postgres://Out/Dest"}
    assert _src(a.result.column_edges[0]) == [("postgres://Src/Data", "Value")]


def test_merge_branches_union_sources_for_same_target_column():
    a = SqlWorker().analyze(
        "MERGE INTO b.t t USING a.s s ON t.id = s.id "
        "WHEN MATCHED AND s.flag = 1 THEN UPDATE SET value = s.updated "
        "WHEN NOT MATCHED THEN INSERT (value) VALUES (s.initial)"
    )
    edge = _edges_by_target(a.result)["value"]
    assert _src(edge) == [("glue://a/s", "initial"), ("glue://a/s", "updated")]
    assert ("glue://a/s", "flag") in [(r.dataset_id, r.name) for r in edge.indirect_sources]


def test_merge_subquery_resolves_renamed_columns_and_all_tables():
    a = SqlWorker().analyze(
        "MERGE INTO b.t t USING (SELECT s.id, s.x + u.y AS total "
        "FROM a.s s JOIN a.u u ON s.id = u.id) q ON t.id = q.id "
        "WHEN MATCHED THEN UPDATE SET value = q.total"
    )
    assert _src(_edges_by_target(a.result)["value"]) == [("glue://a/s", "x"), ("glue://a/u", "y")]
    assert {e.source for e in a.result.table_edges} >= {"glue://a/s", "glue://a/u"}


def test_update_from_resolves_source_alias():
    a = SqlWorker().analyze(
        "UPDATE b.t AS t SET value = s.x FROM a.s AS s WHERE t.id = s.id",
        dialect="postgres",
    )
    assert _src(a.result.column_edges[0]) == [("glue://a/s", "x")]
    assert "glue://a/s" in a.inputs


def test_invalid_dialect_and_unreadable_file_report_unresolved(tmp_path):
    assert SqlWorker().analyze("SELECT x FROM a.s", dialect="not_a_dialect").result.unresolved
    assert SqlWorker().analyze_file(tmp_path / "missing.sql").result.unresolved


def test_union_constant_and_count_star_do_not_invent_sources():
    a = SqlWorker().analyze("CREATE TABLE b.t AS SELECT 1 AS x UNION ALL SELECT 2 AS x")
    assert a.result.unresolved == []
    assert a.result.column_edges[0].provenance.confidence == "exact"
    a = SqlWorker().analyze("CREATE TABLE b.t AS SELECT COUNT(*) AS n FROM a.s")
    assert not a.result.column_edges[0].sources
    assert a.result.unresolved == []


def test_unknown_and_ambiguous_columns_are_not_exact_sources():
    worker = SqlWorker(DictSchemaProvider({"a": {"s": ["exists"]}}))
    result = worker.analyze("CREATE TABLE b.t AS SELECT missing FROM a.s").result
    assert result.unresolved[0].kind == "unknown_column"
    assert result.column_edges[0].provenance.confidence == "partial"
    assert result.column_edges[0].sources == []
    result = (
        SqlWorker().analyze("CREATE TABLE b.t AS SELECT x FROM a.s JOIN a.u ON s.id=u.id").result
    )
    assert result.column_edges[0].provenance.confidence == "partial"
    assert result.unresolved


def test_glue_schema_lookup_is_cached_and_includes_partitions():
    from etl_parser.workers.sql import GlueSchemaProvider

    class Client:
        calls = 0

        class exceptions:
            class EntityNotFoundException(Exception):
                pass

        def get_table(self, **kwargs):
            self.calls += 1
            return {
                "Table": {
                    "StorageDescriptor": {"Columns": [{"Name": "x"}]},
                    "PartitionKeys": [{"Name": "day"}],
                }
            }

    client = Client()
    schema = GlueSchemaProvider(client)
    assert schema.columns("glue://a/t") == ["x", "day"]
    assert schema.columns("glue://a/t") == ["x", "day"]
    assert schema.columns("postgres://a/t") is None
    assert client.calls == 1
