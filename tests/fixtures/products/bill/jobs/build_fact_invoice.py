"""Materialise the curated invoice fact from rated invoices.

Source: bill_stg.invoices_rated
Target: bill_cur.fact_invoice
Owner: billing-team@utility.example.com
Grain: one row per invoice
Schedule: 0 4 * * *
Dependencies: rate_invoices
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine


def main() -> None:
    engine = create_engine("postgresql+psycopg2://etl@bill-warehouse:5432/bill")

    # Rated invoices produced by rate_invoices.
    rated = pd.read_sql_table("invoices_rated", engine, schema="bill_stg")

    fact = rated[
        ["invoice_id", "account_id", "tariff_id", "kwh", "amount", "status", "issue_date"]
    ].copy()

    # One row per invoice — enforce the grain.
    fact = fact.drop_duplicates(subset=["invoice_id"])

    fact.to_sql(
        "fact_invoice", engine, schema="bill_cur", if_exists="replace", index=False
    )


if __name__ == "__main__":
    main()
