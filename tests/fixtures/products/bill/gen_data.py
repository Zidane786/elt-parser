"""Generate FK-consistent fixture CSVs for the billing (bill) data product.

STDLIB ONLY (csv, random, datetime) so it runs with no dependencies installed:

    python data_products/bill/gen_data.py

Writes, under data/ next to this file:
  * billing_pg.invoices.csv  — source invoice ledger  (~4,000 rows)
  * bill_cur.fact_invoice.csv — curated invoice fact   (mirror of the ledger)

Rows draw ids from the canonical id ranges (copied from the shared FK
contract below) so cross-product joins resolve against customer_master
(account_id) and tariff_pricing (tariff_id).
"""
from __future__ import annotations

import csv
import datetime as dt
import os
import random

# --- Canonical ID ranges (from canonical _contract) -----------------------
# Copied verbatim from data_products/catalog/_contract.py so this repo is
# self-contained (no cross-repo import). These MUST match the producers.
ACCOUNT_IDS = range(100_000, 102_500)   # from canonical _contract (customer_master)
TARIFF_IDS = range(600_000, 600_040)    # from canonical _contract (tariff_pricing)
INVOICE_IDS = range(700_000, 730_000)   # from canonical _contract (billing owns this)
# ---------------------------------------------------------------------------

INVOICE_STATUS = ["draft", "issued", "paid", "overdue", "void"]

SEED = 60601  # fixed, unique per product (bill)
N_INVOICES = 4000

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")


def _write_csv(name: str, header: list[str], rows: list[list]) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, name)
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def main() -> None:
    random.seed(SEED)

    accounts = list(ACCOUNT_IDS)
    tariffs = list(TARIFF_IDS)
    invoice_ids = random.sample(list(INVOICE_IDS), N_INVOICES)

    start = dt.date(2025, 1, 1)

    header = [
        "invoice_id", "account_id", "tariff_id", "amount", "kwh", "status", "issue_date"
    ]
    rows: list[list] = []
    for inv_id in invoice_ids:
        account_id = random.choice(accounts)
        tariff_id = random.choice(tariffs)
        kwh = round(random.uniform(50.0, 2500.0), 2)
        unit_rate = round(random.uniform(0.08, 0.32), 4)
        amount = round(kwh * unit_rate + random.uniform(5.0, 20.0), 2)
        status = random.choice(INVOICE_STATUS)
        issue_date = (start + dt.timedelta(days=random.randint(0, 364))).isoformat()
        rows.append([inv_id, account_id, tariff_id, amount, kwh, status, issue_date])

    # Source ledger.
    p1 = _write_csv("billing_pg.invoices.csv", header, rows)

    # Curated fact mirrors the ledger grain (one row per invoice).
    p2 = _write_csv("bill_cur.fact_invoice.csv", header, rows)

    print(f"wrote {len(rows)} rows -> {p1}")
    print(f"wrote {len(rows)} rows -> {p2}")


if __name__ == "__main__":
    main()
