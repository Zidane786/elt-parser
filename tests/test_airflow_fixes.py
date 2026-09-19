"""Regression tests for the Airflow worker fixes of review findings 5, 6, 15, 16 and WP-D."""

import pytest

from etl_parser.scanner.repo import RepoScanner
from etl_parser.workers.airflow import AirflowWorker

HEADER = (
    "from airflow import DAG\n"
    "from airflow.operators.bash import BashOperator\n"
    "from airflow.operators.empty import EmptyOperator\n"
    "from airflow.utils.task_group import TaskGroup\n"
    "from airflow.decorators import dag, task\n"
    "from airflow.providers.amazon.aws.operators.athena import AthenaOperator\n"
)


def analyze(tmp_path, body, scripts=()):
    for name in scripts:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'spark.table("db.{path.stem}")')
    (tmp_path / "dag.py").write_text(HEADER + body)
    index = RepoScanner(tmp_path).scan()
    source = next(f for f in index.files if f.path == "dag.py")
    return AirflowWorker(index).analyze_source(source)


def tasks(result):
    return {s.id: s for s in result.schedules.values() if s.task_id is not None}


def upstream(result, dag_id, task_id):
    return result.schedules[f"airflow.dag.{dag_id}.{task_id}"].declared_upstream


# ------------------------------------------------------- constructor DAGs (5)
def test_dag_kwarg_binds_tasks_to_the_named_dag(tmp_path):
    result = analyze(
        tmp_path,
        'dag1 = DAG("first", schedule_interval="0 1 * * *")\n'
        'dag2 = DAG("second", schedule_interval="0 2 * * *")\n'
        'a = BashOperator(task_id="a", bash_command="python a.py", dag=dag1)\n'
        'b = BashOperator(task_id="b", bash_command="python b.py", dag=dag2)\n'
        "with dag1:\n"
        '    c = BashOperator(task_id="c", bash_command="python c.py")\n'
        "a >> c\n",
        scripts=("a.py", "b.py", "c.py"),
    )
    assert set(tasks(result)) == {
        "airflow.dag.first.a",
        "airflow.dag.second.b",
        "airflow.dag.first.c",
    }
    assert result.task_jobs["airflow.dag.first.a"] == "a"
    assert result.task_jobs["airflow.dag.second.b"] == "b"
    assert result.schedules["airflow.dag.first.c"].cron == "0 1 * * *"
    assert upstream(result, "first", "c") == ["airflow.dag.first.a"]
    assert result.unresolved == []


def test_two_constructor_dags_without_kwarg_use_the_enclosing_dag(tmp_path):
    result = analyze(
        tmp_path,
        'dag = DAG("only", schedule="@daily")\n'
        'a = BashOperator(task_id="a", bash_command="python a.py")\n',
        scripts=("a.py",),
    )
    assert set(tasks(result)) == {"airflow.dag.only.a"}


# ------------------------------------------------------------ control flow (6)
def test_literal_loop_is_unrolled_into_resolved_tasks(tmp_path):
    result = analyze(
        tmp_path,
        'dag2 = DAG("second", schedule="@daily")\n'
        'for n in ["x", "y"]:\n'
        '    BashOperator(task_id=f"loop_{n}", bash_command=f"python jobs/{n}.py", dag=dag2)\n',
        scripts=("jobs/x.py", "jobs/y.py"),
    )
    assert result.task_jobs == {
        "airflow.dag.second.loop_x": "jobs/x",
        "airflow.dag.second.loop_y": "jobs/y",
    }
    assert result.unresolved == []


def test_unknown_loop_iterable_is_one_dynamic_schedule_naming_the_pattern(tmp_path):
    result = analyze(
        tmp_path,
        'dag2 = DAG("second", schedule="@daily")\n'
        "for n in Variable.get('names'):\n"
        '    BashOperator(task_id=f"loop_{n}", bash_command=f"python jobs/{n}.py", dag=dag2)\n',
    )
    assert tasks(result) == {}
    assert [u.kind for u in result.unresolved] == ["dynamic_schedule"]
    assert "loop_{n}" in result.unresolved[0].expression
    assert result.unresolved[0].line == 9


def test_while_try_and_comprehension_bodies_are_visited(tmp_path):
    result = analyze(
        tmp_path,
        'with DAG("d", schedule="@daily"):\n'
        "    while pending():\n"
        '        w = EmptyOperator(task_id="w")\n'
        "    try:\n"
        '        t = EmptyOperator(task_id="t")\n'
        "    except Exception:\n"
        '        h = EmptyOperator(task_id="h")\n'
        "    finally:\n"
        '        f = EmptyOperator(task_id="f")\n'
        '    group = [BashOperator(task_id=f"c_{n}", bash_command=f"python {n}.py")'
        ' for n in ("a", "b")]\n'
        '    done = EmptyOperator(task_id="done")\n'
        "    group >> done\n"
        "    [w, t] >> done\n",
        scripts=("a.py", "b.py"),
    )
    assert {s.task_id for s in tasks(result).values()} == {"w", "t", "h", "f", "c_a", "c_b", "done"}
    assert result.task_jobs["airflow.dag.d.c_a"] == "a"
    assert upstream(result, "d", "done") == [
        "airflow.dag.d.c_a",
        "airflow.dag.d.c_b",
        "airflow.dag.d.t",
        "airflow.dag.d.w",
    ]


def test_partial_expand_registers_task_and_dynamic_schedule(tmp_path):
    result = analyze(
        tmp_path,
        'with DAG("d", schedule="@daily"):\n'
        '    m = BashOperator.partial(task_id="mapped").expand(bash_command=cmds)\n'
        '    done = EmptyOperator(task_id="done")\n'
        "    m >> done\n",
    )
    assert "airflow.dag.d.mapped" in tasks(result)
    assert result.task_jobs["airflow.dag.d.mapped"] is None
    assert [u.kind for u in result.unresolved] == ["dynamic_schedule"]
    assert upstream(result, "d", "done") == ["airflow.dag.d.mapped"]


def test_task_group_prefixes_ids_and_group_level_dependencies_expand(tmp_path):
    result = analyze(
        tmp_path,
        'with DAG("d", schedule="@daily"):\n'
        '    start = EmptyOperator(task_id="start")\n'
        '    with TaskGroup("group") as tg:\n'
        '        a = BashOperator(task_id="a", bash_command="python a.py")\n'
        '        with TaskGroup(group_id="inner") as inner:\n'
        '            b = BashOperator(task_id="b", bash_command="python b.py")\n'
        "        a >> b\n"
        '    done = EmptyOperator(task_id="done")\n'
        "    start >> tg >> done\n",
        scripts=("a.py", "b.py"),
    )
    ids = set(tasks(result))
    assert {"airflow.dag.d.group.a", "airflow.dag.d.group.inner.b"} <= ids
    assert result.task_jobs["airflow.dag.d.group.inner.b"] == "b"
    assert upstream(result, "d", "group.a") == ["airflow.dag.d.start"]
    assert upstream(result, "d", "group.inner.b") == [
        "airflow.dag.d.group.a",
        "airflow.dag.d.start",
    ]
    assert upstream(result, "d", "done") == [
        "airflow.dag.d.group.a",
        "airflow.dag.d.group.inner.b",
    ]


def test_task_decorator_variants_and_custom_operators(tmp_path):
    result = analyze(
        tmp_path,
        "class MyCustomThing:\n    pass\n"
        '@dag(schedule="@hourly")\n'
        "def pipeline():\n"
        "    @task.python\n"
        "    def first():\n"
        '        spark.table("db.raw").write.saveAsTable("db.stage")\n'
        "    @task.virtualenv(requirements=['x'])\n"
        "    def second(upstream):\n"
        '        spark.table("db.stage").write.saveAsTable("db.final")\n'
        '    custom = MyCustomThing(task_id="custom", thing=1)\n'
        "    a = first()\n"
        "    b = second(a)\n"
        "    b >> custom\n"
        "pipeline()\n",
    )
    ids = tasks(result)
    assert set(ids) == {
        "airflow.dag.pipeline.first",
        "airflow.dag.pipeline.second",
        "airflow.dag.pipeline.custom",
    }
    assert {j.id for j in result.jobs} == {
        "airflow.dag.pipeline.first",
        "airflow.dag.pipeline.second",
    }
    assert result.task_jobs["airflow.dag.pipeline.custom"] is None
    assert upstream(result, "pipeline", "second") == ["airflow.dag.pipeline.first"]
    assert upstream(result, "pipeline", "custom") == ["airflow.dag.pipeline.second"]


# ---------------------------------------------------------- variable reuse (16)
def test_variable_reuse_binds_at_assignment_time(tmp_path):
    result = analyze(
        tmp_path,
        'with DAG("d", schedule="@daily"):\n'
        '    one = EmptyOperator(task_id="one")\n'
        '    two = EmptyOperator(task_id="two")\n'
        '    three = EmptyOperator(task_id="three")\n'
        "    t = one\n"
        "    t >> two\n"
        "    t = three\n"
        "    two >> t\n",
    )
    assert upstream(result, "d", "two") == ["airflow.dag.d.one"]
    assert upstream(result, "d", "three") == ["airflow.dag.d.two"]
    assert upstream(result, "d", "one") == []


def test_assignment_from_a_plain_call_binds_nothing_and_terminates(tmp_path):
    result = analyze(
        tmp_path,
        "import os\n"
        'INTERVAL = os.getenv("SCHEDULE", "@daily")\n'
        'with DAG("d", schedule=INTERVAL):\n'
        '    a = EmptyOperator(task_id="a")\n'
        "    helper = compute()\n"
        "    helper >> a\n",
    )
    assert set(tasks(result)) == {"airflow.dag.d.a"}
    assert upstream(result, "d", "a") == []
    assert all(u.kind != "unsupported_syntax" for u in result.unresolved)


# --------------------------------------------------- SQL operators, offsets (15)
def test_embedded_sql_lines_anchor_to_the_string_constant(tmp_path):
    result = analyze(
        tmp_path,
        'with DAG("d", schedule="@daily"):\n'
        "    q = AthenaOperator(\n"
        '        task_id="q",\n'
        '        database="b",\n'
        '        query="""\n'
        "CREATE TABLE b.t AS\n"
        "SELECT x FROM a.s\n"
        "WHERE dt = '{{ ds }}'\n"
        '""",\n'
        "    )\n",
    )
    edge = result.column_edges[0]
    assert edge.transformation.line_start == 12 and edge.transformation.line_end == 14
    assert [(u.kind, u.line) for u in result.unresolved] == [("dynamic_sql", 14)]
    assert result.table_edges[0].line == 12


def test_sql_list_and_sql_file_path_on_operators(tmp_path):
    (tmp_path / "sql").mkdir()
    (tmp_path / "sql" / "load.sql").write_text("CREATE TABLE b.f AS\nSELECT z FROM a.z")
    result = analyze(
        tmp_path,
        "from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator\n"
        'with DAG("d", schedule="@daily"):\n'
        "    many = AthenaOperator(\n"
        '        task_id="many",\n'
        '        query=["CREATE TABLE b.one AS SELECT x FROM a.s",\n'
        '               "CREATE TABLE b.two AS SELECT y FROM b.one"],\n'
        "    )\n"
        '    f = AthenaOperator(task_id="f", query="sql/load.sql")\n'
        '    missing = AthenaOperator(task_id="missing", query="sql/nope.sql")\n',
    )
    many = next(j for j in result.jobs if j.name == "many")
    assert many.inputs == ["glue://a/s", "glue://b/one"]
    assert many.outputs == ["glue://b/one", "glue://b/two"]
    lines = sorted(e.transformation.line_start for e in result.column_edges if e.job_id == many.id)
    assert lines == [11, 12]
    file_job = next(j for j in result.jobs if j.name == "f")
    assert file_job.outputs == ["glue://b/f"]
    file_edge = next(e for e in result.column_edges if e.job_id == file_job.id)
    assert file_edge.transformation.source_file == "sql/load.sql"
    assert file_edge.transformation.line_start == 1
    assert result.task_jobs["airflow.dag.d.missing"] is None
    assert [u.kind for u in result.unresolved] == ["unresolved_import"]


def test_six_field_cron_is_dynamic_schedule(tmp_path):
    result = analyze(
        tmp_path,
        'with DAG("d", schedule="0 0 1 * * *"):\n    a = EmptyOperator(task_id="a")\n',
    )
    assert result.schedules["airflow.dag.d"].cron is None
    assert result.schedules["airflow.dag.d"].interval_text == "0 0 1 * * *"
    assert [u.kind for u in result.unresolved] == ["dynamic_schedule"]


def test_analyze_file_outside_root_does_not_raise(tmp_path):
    outside = tmp_path.parent / f"{tmp_path.name}_outside_dag.py"
    outside.write_text(HEADER + 'with DAG("d"):\n    a = EmptyOperator(task_id="a")\n')
    (tmp_path / "x.py").write_text("x = 1")
    result = AirflowWorker(RepoScanner(tmp_path).scan()).analyze_file(outside)
    assert "airflow." in next(iter(result.schedules))
    outside.unlink()


@pytest.mark.parametrize(
    "body",
    [
        "",
        "def (",
        "with DAG():\n    pass\n",
        "with DAG(dag_id=x):\n    BashOperator()\n",
        "dag = DAG()\nBashOperator(task_id=1, dag=dag)\n",
        "with DAG('d'):\n    a = BashOperator(task_id='a', bash_command=1)\n"
        "    a >> a >> [a, [a]]\n",
        "with DAG('d'):\n    for a, b in x:\n        BashOperator(task_id=a)\n",
        "with DAG('d'):\n    for (a, b) in [(1, 2), 3]:\n        BashOperator(task_id=f'{a}')\n",
        "with DAG('d'):\n    [BashOperator(task_id=n) for n in None]\n",
        "with DAG('d'):\n    BashOperator.partial().expand()\n",
        "with DAG('d'):\n    with TaskGroup() as g:\n        g >> g\n",
        "with DAG('d'):\n    chain()\n    chain([], [1])\n    cross_downstream(x, y)\n",
        "with DAG('d'):\n    a = EmptyOperator(task_id='a')\n    a.set_upstream()\n"
        "    a.set_downstream(b)\n",
        "with DAG('d', schedule=timedelta()):\n"
        "    AthenaOperator(task_id='q', query=['x', 1, None])\n",
        "with DAG('d'):\n    AthenaOperator(task_id='q', query=f'{x}')\n",
        "with DAG('d'):\n    AthenaOperator(task_id='q', query='.sql')\n",
        "with DAG('d'):\n    SQLExecuteQueryOperator(task_id='q', sql='SELECT 1')\n",
        "@dag\ndef p():\n    @task.python\n    def f(): pass\n    f() >> f()\np()\n",
        "with DAG('d'):\n    t = None\n    t >> t\n    t = [None]\n    t << t\n",
        "with DAG('d'):\n    PythonOperator(task_id='p', python_callable=missing)\n",
    ],
)
def test_malformed_dag_sources_never_raise(tmp_path, body):
    result = analyze(tmp_path, body)
    assert result is not None
