"""Re-rate raw invoices against measured consumption, account, and tariff.

Source: bill_raw.invoices, meter_cur.fact_consumption, cust_cur.dim_account, tariff_cur.dim_tariff
Target: bill_stg.invoices_rated
Owner: billing-team@utility.example.com
Grain: one row per issued invoice
Schedule: 0 3 * * *
Dependencies: ingest_invoices
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine


def main() -> None:
    engine = create_engine("postgresql+psycopg2://etl@bill-warehouse:5432/bill")

    # Raw invoices landed by ingest_invoices.
    invoices = pd.read_sql_table("invoices", engine, schema="bill_raw")

    # Measured daily consumption (cross-product: smart_metering).
    consumption = pd.read_sql(
        "SELECT account_id, meter_id, day, kwh AS metered_kwh "
        "FROM meter_cur.fact_consumption",
        engine,
    )

    # Account dimension (cross-product: customer_master).
    accounts = pd.read_sql(
        "SELECT account_id, tier, status AS account_status FROM cust_cur.dim_account",
        engine,
    )

    # Tariff dimension (cross-product: tariff_pricing).
    tariffs = pd.read_sql(
        "SELECT tariff_id, unit_rate, standing_charge FROM tariff_cur.dim_tariff",
        engine,
    )

    # Roll measured consumption up to per-account billed kWh.
    metered = (
        consumption.groupby("account_id", as_index=False)["metered_kwh"].sum()
    )

    # Join invoices -> metered consumption -> account dim -> tariff dim.
    rated = invoices.merge(metered, on="account_id", how="left")
    rated = rated.merge(accounts, on="account_id", how="left")
    rated = rated.merge(tariffs, on="tariff_id", how="left")

    # Recompute the amount from rated kWh against the tariff.
    rated["kwh"] = rated["metered_kwh"].fillna(rated["kwh"])
    rated["amount"] = (rated["kwh"] * rated["unit_rate"]).fillna(rated["amount"]) + \
        rated["standing_charge"].fillna(0.0)

    out = rated[
        ["invoice_id", "account_id", "tariff_id", "kwh", "amount", "status", "issue_date"]
    ]

    out.to_sql(
        "invoices_rated", engine, schema="bill_stg", if_exists="replace", index=False
    )


if __name__ == "__main__":
    main()
