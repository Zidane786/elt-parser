"""Extract the invoice ledger from billing_pg via JDBC into the RAW layer.

Source: billing_pg.invoices
Target: bill_raw.invoices
Owner: billing-team@utility.example.com
Grain: one row per issued invoice
Schedule: 0 1 * * *
Dependencies: none
"""
from __future__ import annotations

import datetime as dt

import pandas as pd
from sqlalchemy import create_engine


def main() -> None:
    # Operational billing Postgres (system of record for the invoice ledger).
    src_engine = create_engine("postgresql+psycopg2://etl@billing-pg:5432/billing")
    # Athena-backed warehouse connection (raw landing zone).
    raw_engine = create_engine("postgresql+psycopg2://etl@bill-warehouse:5432/bill")

    # Pull the full invoice ledger from billing_pg.invoices.
    invoices = pd.read_sql(
        "SELECT invoice_id, account_id, tariff_id, amount, kwh, status, issue_date "
        "FROM billing_pg.invoices",
        src_engine,
    )

    # Stamp the ingest partition.
    invoices["load_date"] = dt.date.today().isoformat()

    # Land into bill_raw.invoices.
    invoices.to_sql("invoices", raw_engine, schema="bill_raw", if_exists="replace", index=False)


if __name__ == "__main__":
    main()
