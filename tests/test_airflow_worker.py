from etl_parser.pipeline import scan


def test_airflow_list_chain_and_unmapped_task_bridge(tmp_path):
    for name in ("a", "b", "c"):
        (tmp_path / f"{name}.py").write_text(f'spark.table("db.{name}")')
    (tmp_path / "dag.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.operators.empty import EmptyOperator\n"
        'with DAG("pipeline", schedule="@daily"):\n'
        '    a = BashOperator(task_id="a", bash_command="python a.py")\n'
        '    b = BashOperator(task_id="b", bash_command="python b.py")\n'
        '    gate = EmptyOperator(task_id="gate")\n'
        '    c = BashOperator(task_id="c", bash_command="python c.py")\n'
        "    [a, b] >> gate >> c\n"
    )
    doc = scan(tmp_path).document
    assert {d.job_id for d in doc.job_dependencies["c"]} == {"a", "b"}
    assert all(d.sources == ["dag"] for d in doc.job_dependencies["c"])
    assert next(s for s in doc.schedules.values() if s.task_id == "c").cron == "0 0 * * *"


def test_taskflow_functions_have_distinct_jobs_and_dependencies(tmp_path):
    (tmp_path / "dag.py").write_text(
        "from airflow.decorators import dag, task\n"
        '@dag(schedule="@hourly")\ndef pipeline():\n'
        '    @task\n    def first():\n        spark.table("db.raw").write.saveAsTable("db.stage")\n'
        "    @task\n    def second(upstream):\n"
        '        spark.table("db.stage").write.saveAsTable("db.final")\n'
        "    a = first()\n    b = second(a)\n"
        "pipeline()\n"
    )
    doc = scan(tmp_path).document
    assert len(doc.jobs) == 2
    first, second = sorted(doc.jobs, key=lambda j: j.name)
    assert first.outputs == ["glue://db/stage"]
    assert second.outputs == ["glue://db/final"]
    assert doc.job_dependencies[second.id][0].sources == ["dag", "data"]


def test_multiple_dags_do_not_mix_reused_variable_names(tmp_path):
    for name in ("first_a", "first_b", "second_a", "second_b"):
        (tmp_path / f"{name}.py").write_text(f'spark.table("db.{name}")')
    source = "from airflow import DAG\nfrom airflow.operators.bash import BashOperator\n"
    for dag in ("first", "second"):
        source += (
            f'with DAG("{dag}", schedule="@daily"):\n'
            f'    a = BashOperator(task_id="a", bash_command="python {dag}_a.py")\n'
            f'    b = BashOperator(task_id="b", bash_command="python {dag}_b.py")\n'
            "    a >> b\n"
        )
    (tmp_path / "dag.py").write_text(source)
    doc = scan(tmp_path).document
    assert [d.job_id for d in doc.job_dependencies["first_b"]] == ["first_a"]
    assert [d.job_id for d in doc.job_dependencies["second_b"]] == ["second_a"]


def test_external_task_sensor_connects_local_dags(tmp_path):
    (tmp_path / "producer.py").write_text('spark.table("db.source")')
    (tmp_path / "consumer.py").write_text('spark.table("db.unrelated")')
    (tmp_path / "dag.py").write_text(
        "from airflow import DAG\n"
        "from airflow.operators.bash import BashOperator\n"
        "from airflow.sensors.external_task import ExternalTaskSensor\n"
        'with DAG("producer"):\n'
        '    a = BashOperator(task_id="produce", bash_command="python producer.py")\n'
        'with DAG("consumer"):\n'
        '    gate = ExternalTaskSensor(task_id="wait", external_dag_id="producer",\n'
        '                              external_task_id="produce")\n'
        '    b = BashOperator(task_id="consume", bash_command="python consumer.py")\n'
        "    gate >> b\n"
    )
    doc = scan(tmp_path).document
    assert [d.job_id for d in doc.job_dependencies["consumer"]] == ["producer"]
