"""Airflow DAG for the smart_metering (meter) pipeline.

One task per ETL script under glue/, wired by the registry depends_on edges:

    ingest_interval_reads >> stage_reads >> build_fact_consumption

The DAG-level schedule is the product primary schedule (daily 02:00). The
hourly ingest is modelled here as an upstream task in the daily run; in
production ingest_interval_reads also runs on its own hourly trigger.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

GLUE_DIR = "glue"

default_args = {
    "owner": "smart-metering-team",
    "email": ["smart-metering-team@utility.example.com"],
    "retries": 1,
}

with DAG(
    dag_id="meter_pipeline",
    description="smart_metering: land interval reads -> clean -> daily consumption fact.",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    default_args=default_args,
    tags=["meter", "smart_metering", "pyspark"],
) as dag:

    ingest_interval_reads = BashOperator(
        task_id="ingest_interval_reads",
        bash_command=f"spark-submit {GLUE_DIR}/ingest_interval_reads.py",
    )

    stage_reads = BashOperator(
        task_id="stage_reads",
        bash_command=f"spark-submit {GLUE_DIR}/stage_reads.py",
    )

    build_fact_consumption = BashOperator(
        task_id="build_fact_consumption",
        bash_command=f"spark-submit {GLUE_DIR}/build_fact_consumption.py",
    )

    ingest_interval_reads >> stage_reads >> build_fact_consumption
