"""Regression tests for the SQL worker fixes of review findings 7, 14, 15, 25 and WP-D."""

import ast

import pytest

from etl_parser.workers.base import parse_header, sql_line_offset
from etl_parser.workers.sql import DictSchemaProvider, SqlWorker


def _edges_by_target(result):
    return {e.target.name: e for e in result.column_edges}


def _src(edge):
    return sorted((s.dataset_id, s.name) for s in edge.sources)


def _pairs(result):
    return [
        ((j.left.dataset_id, j.left.name), (j.right.dataset_id, j.right.name))
        for j in result.join_conditions
    ]


# ------------------------------------------------------------------ Jinja (7)
def test_jinja_literal_hole_keeps_clean_statement_and_analyses_partial():
    sql = (
        "CREATE TABLE b.first AS SELECT x FROM a.s;\n"
        "CREATE TABLE b.second AS SELECT y FROM a.t\n"
        "WHERE dt = '{{ ds }}'"
    )
    a = SqlWorker().analyze(sql, dialect="trino", engine="athena", job_id="j")
    assert a.outputs == {"glue://b/first", "glue://b/second"}
    edges = _edges_by_target(a.result)
    assert edges["x"].provenance.confidence == "exact"
    assert edges["y"].provenance.confidence == "partial"
    assert _src(edges["y"]) == [("glue://a/t", "y")]
    assert ("glue://a/t", "dt") in [(r.dataset_id, r.name) for r in edges["y"].indirect_sources]
    assert "{{ ds }}" in edges["y"].transformation.expression or "ds" in " ".join(
        u.symbols[0] for u in a.result.unresolved
    )
    assert [(u.kind, u.line, u.symbols) for u in a.result.unresolved] == [
        ("dynamic_sql", 3, ["ds"])
    ]


def test_jinja_hole_in_table_identity_is_dynamic_for_that_statement_only():
    sql = "CREATE TABLE b.first AS SELECT x FROM a.s;\nCREATE TABLE b.second AS SELECT y FROM {{ table }}"
    a = SqlWorker().analyze(sql, dialect="trino", job_id="j")
    assert a.outputs == {"glue://b/first"}
    assert [(u.kind, u.line) for u in a.result.unresolved] == [("dynamic_sql", 2)]
    assert a.result.unresolved[0].symbols == ["table"]


def test_jinja_hole_in_projection_or_alias_is_identity():
    for sql in (
        "CREATE TABLE b.t AS SELECT {{ cols }} FROM a.s",
        "CREATE TABLE b.t AS SELECT x AS {{ alias }} FROM a.s",
        "CREATE TABLE {{ target }} AS SELECT x FROM a.s",
    ):
        a = SqlWorker().analyze(sql, dialect="trino", job_id="j")
        assert a.result.column_edges == []
        assert [u.kind for u in a.result.unresolved] == ["dynamic_sql"]


def test_bare_jinja_hole_in_predicate_is_a_value_not_a_column():
    a = SqlWorker().analyze(
        "CREATE TABLE b.t AS SELECT x FROM a.s WHERE amount > {{ params.threshold }}",
        dialect="trino",
        job_id="j",
    )
    edge = a.result.column_edges[0]
    assert edge.provenance.confidence == "partial"
    assert [(r.dataset_id, r.name) for r in edge.indirect_sources] == [("glue://a/s", "amount")]
    assert a.result.unresolved[0].symbols == ["params.threshold"]


def test_shell_style_hole_and_jinja_blocks():
    a = SqlWorker().analyze("SELECT x FROM a.s WHERE d = '${DT}'", dialect="trino", job_id="j")
    assert a.inputs == {"glue://a/s"}
    assert a.result.unresolved[0].kind == "dynamic_sql"
    a = SqlWorker().analyze("{% if x %}SELECT 1{% endif %}", dialect="trino", job_id="j")
    assert [u.kind for u in a.result.unresolved] == ["dynamic_sql"]


# ------------------------------------------------------------- temp tables (14)
def test_temp_tables_are_not_datasets_and_edges_are_repointed():
    sql = (
        "CREATE TEMPORARY TABLE tmp AS SELECT x, k FROM a.s WHERE active = 1;\n"
        "CREATE TABLE mart.t AS SELECT t.x FROM tmp t JOIN a.u u ON t.k = u.k"
    )
    a = SqlWorker().analyze(sql, dialect="spark", engine="spark", job_id="j", default_db="default")
    assert a.inputs == {"glue://a/s", "glue://a/u"}
    assert a.outputs == {"glue://mart/t"}
    assert "glue://default/tmp" not in {d.id for d in a.result.datasets}
    assert sorted((t.source, t.target) for t in a.result.table_edges) == [
        ("glue://a/s", "glue://mart/t"),
        ("glue://a/u", "glue://mart/t"),
    ]
    edge = a.result.column_edges[-1]
    assert _src(edge) == [("glue://a/s", "x")]
    assert _pairs(a.result) == [(("glue://a/s", "k"), ("glue://a/u", "k"))]


def test_temp_view_and_insert_into_temp_stay_session_scoped():
    sql = (
        "CREATE OR REPLACE TEMP VIEW v AS SELECT x FROM a.s;\n"
        "CREATE TEMPORARY TABLE tmp2 (x INT);\n"
        "INSERT INTO tmp2 SELECT x FROM v;\n"
        "CREATE TABLE b.t AS SELECT x FROM tmp2"
    )
    a = SqlWorker().analyze(sql, dialect="spark", engine="spark", job_id="j", default_db="d")
    assert a.inputs == {"glue://a/s"}
    assert a.outputs == {"glue://b/t"}
    assert {d.id for d in a.result.datasets} == {"glue://a/s", "glue://b/t"}
    assert _src(_edges_by_target(a.result)["x"]) == [("glue://a/s", "x")]


# ------------------------------------------------------ UPDATE FROM JOIN (25)
def test_update_from_join_keeps_join_indirect_sources():
    a = SqlWorker().analyze(
        "UPDATE b.t AS t SET a = s.a FROM a.s AS s JOIN a.u AS u ON s.id = u.id WHERE t.id = s.id",
        dialect="postgres",
        engine="postgres",
        job_id="j",
    )
    edge = a.result.column_edges[0]
    assert _src(edge) == [("postgres://a/s", "a")]
    indirect = {(r.dataset_id, r.name) for r in edge.indirect_sources}
    assert {("postgres://a/s", "id"), ("postgres://a/u", "id"), ("postgres://b/t", "id")} <= indirect
    assert a.inputs >= {"postgres://a/s", "postgres://a/u"}
    assert (("postgres://a/s", "id"), ("postgres://a/u", "id")) in _pairs(a.result)


# ------------------------------------------------------- duplicate names (25)
def test_duplicate_output_names_give_one_edge_and_an_unknown_column():
    a = SqlWorker().analyze("CREATE TABLE b.t AS SELECT x, x FROM a.s", dialect="trino", job_id="j")
    assert [e.target.name for e in a.result.column_edges] == ["x"]
    assert [u.kind for u in a.result.unresolved] == ["unknown_column"]
    assert "duplicate" in a.result.unresolved[0].reason.lower()
    schema = DictSchemaProvider({"a": {"s": ["id", "x"], "u": ["id", "y"]}})
    a = SqlWorker(schema).analyze(
        "CREATE TABLE b.t AS SELECT * FROM a.s JOIN a.u ON s.id = u.id", dialect="trino", job_id="j"
    )
    assert sorted(e.target.name for e in a.result.column_edges) == ["id", "x", "y"]
    assert [u.kind for u in a.result.unresolved] == ["unknown_column"]


# --------------------------------------------------------------- new models
def test_merge_delete_keeps_using_source_as_input_and_join_condition():
    a = SqlWorker().analyze(
        "MERGE INTO b.t t USING a.s s ON t.id = s.id WHEN MATCHED THEN DELETE",
        dialect="spark",
        engine="spark",
        job_id="j",
    )
    assert a.inputs == {"glue://a/s", "glue://b/t"} and a.outputs == {"glue://b/t"}
    assert [(t.source, t.target) for t in a.result.table_edges] == [("glue://a/s", "glue://b/t")]
    assert _pairs(a.result) == [(("glue://a/s", "id"), ("glue://b/t", "id"))]
    assert a.result.unresolved == []


def test_merge_update_set_star_and_insert_star():
    sql = (
        "MERGE INTO b.t t USING a.s s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    a = SqlWorker(DictSchemaProvider({"b": {"t": ["id", "v"]}})).analyze(
        sql, dialect="spark", engine="spark", job_id="j"
    )
    edges = _edges_by_target(a.result)
    assert _src(edges["v"]) == [("glue://a/s", "v")] and _src(edges["id"]) == [("glue://a/s", "id")]
    assert a.result.unresolved == []
    a = SqlWorker().analyze(sql, dialect="spark", engine="spark", job_id="j")
    assert a.result.column_edges == []
    assert [u.kind for u in a.result.unresolved] == ["missing_schema"]
    assert [(t.source, t.target) for t in a.result.table_edges] == [("glue://a/s", "glue://b/t")]


def test_insert_overwrite_directory_writes_s3_dataset():
    a = SqlWorker().analyze(
        "INSERT OVERWRITE DIRECTORY 's3://bucket/out/' SELECT x FROM a.s",
        dialect="spark",
        engine="spark",
        job_id="j",
    )
    assert a.outputs == {"s3://bucket/out/"}
    edge = a.result.column_edges[0]
    assert edge.target.dataset_id == "s3://bucket/out/" and _src(edge) == [("glue://a/s", "x")]
    assert next(d for d in a.result.datasets if d.id == "s3://bucket/out/").kind == "s3_path"


def test_select_into_and_create_table_like():
    a = SqlWorker().analyze("SELECT x INTO b.t FROM a.s", dialect="postgres", engine="postgres")
    assert a.outputs == {"postgres://b/t"}
    assert _src(a.result.column_edges[0]) == [("postgres://a/s", "x")]
    a = SqlWorker().analyze("CREATE TABLE b.t LIKE a.s", dialect="spark", engine="spark", job_id="j")
    assert a.inputs == {"glue://a/s"} and a.outputs == {"glue://b/t"}
    assert [(t.source, t.target) for t in a.result.table_edges] == [("glue://a/s", "glue://b/t")]
    assert a.result.unresolved == [] and a.result.column_edges == []
    a = SqlWorker().analyze("CREATE TABLE b.t (LIKE a.s)", dialect="postgres", engine="postgres")
    assert [(t.source, t.target) for t in a.result.table_edges] == [
        ("postgres://a/s", "postgres://b/t")
    ]


def test_delete_where_in_subquery_reads_the_subquery_table():
    a = SqlWorker().analyze(
        "DELETE FROM b.t WHERE id IN (SELECT id FROM a.s WHERE flag = 1)",
        dialect="postgres",
        engine="postgres",
    )
    assert a.inputs == {"postgres://a/s", "postgres://b/t"}
    assert a.outputs == {"postgres://b/t"}


def test_location_and_external_location_become_dataset_aliases():
    a = SqlWorker().analyze(
        "CREATE EXTERNAL TABLE b.t (x INT) LOCATION 's3://bucket/t/'", dialect="hive", engine="athena"
    )
    ds = next(d for d in a.result.datasets if d.id == "glue://b/t")
    assert ds.aliases == ["s3://bucket/t/"] and ds.physical_location == "s3://bucket/t/"
    a = SqlWorker().analyze(
        "CREATE TABLE b.t WITH (external_location = 's3://bucket/t2/', format = 'PARQUET') "
        "AS SELECT x FROM a.s",
        dialect="trino",
        engine="athena",
    )
    ds = next(d for d in a.result.datasets if d.id == "glue://b/t")
    assert ds.aliases == ["s3://bucket/t2/"] and ds.physical_location == "s3://bucket/t2/"
    assert ds.columns == ["x"]
    a = SqlWorker().analyze(
        "CREATE TABLE b.t USING parquet LOCATION 's3://bucket/t3' AS SELECT x FROM a.s",
        dialect="spark",
        engine="spark",
    )
    assert next(d for d in a.result.datasets if d.id == "glue://b/t").aliases == ["s3://bucket/t3"]


# ---------------------------------------------------------- join conditions
def test_join_conditions_resolve_through_aliases_and_ctes_sorted_and_deduped():
    sql = (
        "CREATE TABLE b.t AS WITH c AS (SELECT id AS cid, v FROM a.s) "
        "SELECT c.v, u.y FROM c JOIN a.u u ON c.cid = u.id AND u.id = c.cid "
        "JOIN a.w w ON w.k = u.k"
    )
    a = SqlWorker().analyze(sql, dialect="trino", job_id="j")
    assert _pairs(a.result) == [
        (("glue://a/s", "id"), ("glue://a/u", "id")),
        (("glue://a/u", "k"), ("glue://a/w", "k")),
    ]
    j = a.result.join_conditions[0]
    assert j.job_id == "j" and j.provenance.parser == "sqlglot" and j.provenance.dialect == "trino"


def test_non_equality_and_constant_join_predicates_are_not_conditions():
    a = SqlWorker().analyze(
        "CREATE TABLE b.t AS SELECT s.x FROM a.s s JOIN a.u u ON s.id > u.id AND u.flag = 1",
        dialect="trino",
    )
    assert a.result.join_conditions == []


# ---------------------------------------------------------------- base helpers
def test_parse_header_strips_trailing_comment_after_value():
    header = parse_header("-- Owner: me@x  -- primary\n-- Schedule: 0 5 1 * *  -- monthly\nSELECT 1")
    assert header == {"owner": "me@x", "schedule": "0 5 1 * *"}


def test_sql_line_offset_points_at_the_string_constant():
    tree = ast.parse(
        "x = Operator(\n"
        "    task_id='a',\n"
        '    query="""\n'
        "SELECT 1\n"
        '""",\n'
        ")\n"
        "y = f(('SELECT 2'\n"
        "       ' FROM t').format())\n"
        "z = g('a' +\n"
        "      'b')\n"
    )
    call = tree.body[0].value
    assert sql_line_offset(call.keywords[1].value) == 2
    assert sql_line_offset(tree.body[1].value.args[0]) == 6
    assert sql_line_offset(tree.body[2].value.args[0]) == 8
    assert sql_line_offset(None) == 0


# ---------------------------------------------------------------- never raise
@pytest.mark.parametrize(
    "sql",
    [
        "",
        ";;;",
        "{{",
        "}}",
        "{{ }}",
        "${",
        "SELECT '{{ unterminated",
        "SELECT {{ a }} {{ b }}",
        "CREATE TABLE {{ t }} AS SELECT {{ c }} FROM {{ s }} WHERE {{ w }}",
        "MERGE INTO t USING s ON 1 WHEN MATCHED THEN UPDATE SET *",
        "MERGE INTO t USING (SELECT) ON WHEN",
        "UPDATE SET FROM JOIN",
        "INSERT OVERWRITE DIRECTORY SELECT",
        "SELECT INTO FROM",
        "CREATE TABLE LIKE",
        "CREATE TEMPORARY TABLE tmp AS SELECT * FROM tmp",
        "DELETE FROM WHERE IN (SELECT)",
        "SELECT x, x, x FROM a JOIN a ON a.x = a.x",
        "\x00\x01\x02",
        "SELECT " + "1 + " * 2000 + "1",
    ],
)
def test_malformed_sql_never_raises(sql):
    a = SqlWorker().analyze(sql, dialect="spark", engine="spark", job_id="fuzz", default_db="d")
    assert a is not None
