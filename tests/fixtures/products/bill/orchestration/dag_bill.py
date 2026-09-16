"""Airflow DAG for the billing (bill) pipeline.

One task per ETL job, wired per the registry depends_on. The DAG-level
schedule is the product primary schedule (build_fact_invoice cadence).

  ingest_invoices >> rate_invoices >> build_fact_invoice
"""
from __future__ import annotations

import datetime as dt

from airflow import DAG
from airflow.operators.bash import BashOperator

JOBS = "data_products/bill/jobs"

default_args = {
    "owner": "billing-team",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=5),
}

with DAG(
    dag_id="bill_pipeline",
    description="billing medallion pipeline: ingest -> rate -> fact_invoice",
    schedule_interval="0 4 * * *",
    start_date=dt.datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["billing", "bill", "revenue"],
) as dag:
    ingest_invoices = BashOperator(
        task_id="ingest_invoices",
        bash_command=f"python {JOBS}/ingest_invoices.py",
    )

    rate_invoices = BashOperator(
        task_id="rate_invoices",
        bash_command=f"python {JOBS}/rate_invoices.py",
    )

    build_fact_invoice = BashOperator(
        task_id="build_fact_invoice",
        bash_command=f"python {JOBS}/build_fact_invoice.py",
    )

    ingest_invoices >> rate_invoices >> build_fact_invoice
