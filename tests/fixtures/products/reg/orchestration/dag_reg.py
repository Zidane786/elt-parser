"""Airflow DAG for the regulatory_reporting (reg) PySpark + SQL pipeline.

One task per ETL script, wired per the registry depends_on edges:

    stage_reliability_events >> mart_saidi_saifi
    mart_energy_sold            (independent fan-in from billing + smart_metering)
    mart_collections_summary    (independent fan-in from billing + outage_management)

The primary schedule is the monthly reliability-staging cadence (0 4 1 * *).
PySpark steps run via spark-submit; SQL marts run as Athena CTAS statements.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

GLUE_DIR = "glue"
SQL_DIR = "sql"
ATHENA_DB = "reg_cur"

with DAG(
    dag_id="reg_pipeline",
    description="regulatory_reporting marts: SAIDI/SAIFI, energy sold, collections.",
    schedule_interval="0 4 1 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["regulatory_reporting", "reg", "pyspark", "sql"],
) as dag:
    stage_reliability_events = BashOperator(
        task_id="stage_reliability_events",
        bash_command=f"spark-submit {GLUE_DIR}/stage_reliability_events.py",
    )

    mart_saidi_saifi = BashOperator(
        task_id="mart_saidi_saifi",
        bash_command=(
            f"aws athena start-query-execution "
            f"--query-string \"$(cat {SQL_DIR}/mart_saidi_saifi.sql)\" "
            f"--query-execution-context Database={ATHENA_DB}"
        ),
    )

    mart_energy_sold = BashOperator(
        task_id="mart_energy_sold",
        bash_command=(
            f"aws athena start-query-execution "
            f"--query-string \"$(cat {SQL_DIR}/mart_energy_sold.sql)\" "
            f"--query-execution-context Database={ATHENA_DB}"
        ),
    )

    mart_collections_summary = BashOperator(
        task_id="mart_collections_summary",
        bash_command=f"spark-submit {GLUE_DIR}/mart_collections_summary.py",
    )

    # Dependency edges (per registry depends_on).
    stage_reliability_events >> mart_saidi_saifi
    # mart_energy_sold and mart_collections_summary have no upstream deps.
