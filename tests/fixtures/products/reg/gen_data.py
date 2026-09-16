"""Generate FK-consistent seed CSVs for the regulatory_reporting (reg) product.

STDLIB ONLY (csv, random, datetime) so it runs without installing PySpark/Airflow:

    python data_products/reg/gen_data.py

Writes, under ``data/`` (relative to this file):
  * reporting_calendar.csv      — reg_raw.reporting_calendar (source: reporting periods)
  * reliability_events.csv      — reg_stg.reliability_events (conformed outage inputs)
  * mart_saidi_saifi.csv        — reg_cur.mart_saidi_saifi (SAIDI/SAIFI per period/feeder)
  * mart_energy_sold.csv        — reg_cur.mart_energy_sold (energy + revenue per period)
  * mart_collections_summary.csv- reg_cur.mart_collections_summary (collections per period)

regulatory_reporting OWNS no shared id space; it fans in from outage_management
(outage_id, feeder_id), customer_master (account_id) and grid_telemetry (feeder_id).
All FK ids are drawn from the canonical ranges so cross-product joins resolve
against the other repos' generators (e.g. outage_cur.fact_outage, bill_cur.fact_invoice).
"""
from __future__ import annotations

import csv
import datetime as dt
import os
import random

# --- from canonical _contract (data_products/catalog/_contract.py) -----------
# Copied values (this repo is standalone and must NOT import across repos).
ACCOUNT_IDS = range(100_000, 102_500)      # account_id  (originates: customer_master)
FEEDER_IDS = range(500_000, 500_060)       # feeder_id   (originates: grid_telemetry)
OUTAGE_IDS = range(800_000, 802_000)       # outage_id   (originates: outage_management)
# -----------------------------------------------------------------------------

# Pinned shared-key column names (from canonical _contract).
ACCOUNT_ID = "account_id"
FEEDER_ID = "feeder_id"
OUTAGE_ID = "outage_id"

# Categorical sets (mirrored from the registry schema for reg).
REPORT_PERIOD = ["monthly", "quarterly", "annual"]

SEED = 121212  # fixed seed, unique per product (reg)
random.seed(SEED)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

# Reporting horizon: four quarters across 2025-2026.
PERIODS = ["2025-Q3", "2025-Q4", "2026-Q1", "2026-Q2"]
PERIOD_DATES = {
    "2025-Q3": (dt.date(2025, 7, 1), dt.date(2025, 9, 30)),
    "2025-Q4": (dt.date(2025, 10, 1), dt.date(2025, 12, 31)),
    "2026-Q1": (dt.date(2026, 1, 1), dt.date(2026, 3, 31)),
    "2026-Q2": (dt.date(2026, 4, 1), dt.date(2026, 6, 30)),
}

N_RELIABILITY_EVENTS = 2000  # one per outage event, within raw 1k-5k band


def _write_csv(name: str, header: list[str], rows: list[list]) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{name}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return path


def main() -> None:
    feeders = list(FEEDER_IDS)
    accounts = list(ACCOUNT_IDS)
    outage_ids = list(OUTAGE_IDS)
    random.shuffle(outage_ids)

    # --- reg_raw.reporting_calendar (source) ---------------------------------
    calendar_rows: list[list] = []
    for period in PERIODS:
        start, end = PERIOD_DATES[period]
        calendar_rows.append([period, "quarterly", start.isoformat(), end.isoformat()])

    # --- reg_stg.reliability_events ------------------------------------------
    # One conformed row per outage event: feeder_id, outage_id, period, customers, minutes.
    reliability_rows: list[list] = []
    # Aggregators for the SAIDI/SAIFI mart, keyed by (period, feeder_id).
    agg: dict[tuple[str, int], dict[str, float]] = {}
    for outage_id in outage_ids[:N_RELIABILITY_EVENTS]:
        feeder_id = random.choice(feeders)
        period = random.choice(PERIODS)
        customers_affected = random.randint(1, 500)
        minutes = random.randint(2, 600)
        reliability_rows.append([feeder_id, outage_id, period, customers_affected, minutes])

        key = (period, feeder_id)
        a = agg.setdefault(key, {"cust_min": 0.0, "cust": 0, "interruptions": 0})
        a["cust_min"] += minutes * customers_affected
        a["cust"] += customers_affected
        a["interruptions"] += 1

    # --- reg_cur.mart_saidi_saifi --------------------------------------------
    saidi_rows: list[list] = []
    for (period, feeder_id), a in sorted(agg.items()):
        customers = a["cust"]
        saidi = round(a["cust_min"] / customers, 4) if customers else 0.0
        saifi = round(customers / a["interruptions"], 4) if a["interruptions"] else 0.0
        saidi_rows.append([period, feeder_id, saidi, saifi, customers])

    # --- reg_cur.mart_energy_sold --------------------------------------------
    # Energy sold + invoiced revenue per period (reconciles billing x consumption).
    energy_rows: list[list] = []
    for period in PERIODS:
        n_accounts = len(accounts)
        total_kwh = round(sum(random.uniform(150.0, 1200.0) for _ in range(n_accounts)), 2)
        total_revenue = round(total_kwh * random.uniform(0.11, 0.18), 2)
        energy_rows.append([period, total_kwh, total_revenue])

    # --- reg_cur.mart_collections_summary ------------------------------------
    collections_rows: list[list] = []
    for period in PERIODS:
        invoiced = round(random.uniform(800_000.0, 1_500_000.0), 2)
        collection_rate = round(random.uniform(0.82, 0.98), 4)
        collected = round(invoiced * collection_rate, 2)
        collections_rows.append([period, invoiced, collected, collection_rate])

    p1 = _write_csv(
        "reporting_calendar",
        ["period", "period_type", "start_date", "end_date"],
        calendar_rows,
    )
    p2 = _write_csv(
        "reliability_events",
        [FEEDER_ID, OUTAGE_ID, "period", "customers_affected", "minutes"],
        reliability_rows,
    )
    p3 = _write_csv(
        "mart_saidi_saifi",
        ["period", FEEDER_ID, "saidi", "saifi", "customers"],
        saidi_rows,
    )
    p4 = _write_csv(
        "mart_energy_sold",
        ["period", "total_kwh", "total_revenue"],
        energy_rows,
    )
    p5 = _write_csv(
        "mart_collections_summary",
        ["period", "invoiced", "collected", "collection_rate"],
        collections_rows,
    )

    print(f"wrote {len(calendar_rows)} rows -> {p1}")
    print(f"wrote {len(reliability_rows)} rows -> {p2}")
    print(f"wrote {len(saidi_rows)} rows -> {p3}")
    print(f"wrote {len(energy_rows)} rows -> {p4}")
    print(f"wrote {len(collections_rows)} rows -> {p5}")


if __name__ == "__main__":
    main()
