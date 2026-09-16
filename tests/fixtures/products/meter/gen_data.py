"""Generate FK-consistent seed CSVs for the smart_metering (meter) data product.

STDLIB ONLY (csv, random, datetime) so it runs without installing pyspark or
any heavy dependency:

    python data_products/meter/gen_data.py

Writes, under data/:
  * interval_reads.csv  -> meter_raw.interval_reads  (raw source, 15-min reads)
  * fact_consumption.csv -> meter_cur.fact_consumption (daily rollup, curated)

FK consistency: meter_id / account_id / premise_id are drawn from the canonical
ID ranges so cross-product joins (asset_cur.dim_meter, cust_cur.dim_account)
resolve. The fact rows are an exact daily rollup of the interval rows, so the
within-product lineage (reads -> fact) is real too.
"""
from __future__ import annotations

import csv
import os
import random
from datetime import date, datetime, timedelta

# --- Canonical ID ranges (from canonical _contract; copied so this repo stands alone) ---
# from canonical _contract: producers/consumers share the SAME id space so FKs resolve.
ACCOUNT_IDS = range(100_000, 102_500)      # from canonical _contract (customer_master)
PREMISE_IDS = range(200_000, 202_500)      # from canonical _contract (customer_master)
METER_IDS = range(300_000, 302_750)        # from canonical _contract (meter_assets)

# Deterministic output. Unique per-product seed.
random.seed(40404)

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(HERE, "data")

# Number of distinct meters we emit reads for (subset of the meter id space so a
# meter maps to a stable account/premise, matching asset_cur.dim_meter).
N_METERS = 120
# Days of history and reads-per-day (4 intervals/day keeps row count ~1k-5k).
N_DAYS = 10
INTERVALS_PER_DAY = 4
INTERVAL_HOURS = 24 // INTERVALS_PER_DAY

START_DAY = date(2026, 5, 1)


def _meter_population() -> list[dict]:
    """Pick N_METERS meters, each pinned to one account + premise (like dim_meter)."""
    meter_ids = random.sample(list(METER_IDS), N_METERS)
    population = []
    for mid in meter_ids:
        population.append(
            {
                "meter_id": mid,
                "account_id": random.randrange(ACCOUNT_IDS.start, ACCOUNT_IDS.stop),
                "premise_id": random.randrange(PREMISE_IDS.start, PREMISE_IDS.stop),
            }
        )
    return population


def _gen() -> tuple[list[dict], list[dict]]:
    meters = _meter_population()

    interval_rows: list[dict] = []
    # daily[(meter_id, day)] = summed kwh
    daily: dict[tuple[int, str], float] = {}

    for m in meters:
        mid = m["meter_id"]
        for d in range(N_DAYS):
            day = START_DAY + timedelta(days=d)
            day_str = day.isoformat()
            for i in range(INTERVALS_PER_DAY):
                read_ts = datetime(day.year, day.month, day.day, i * INTERVAL_HOURS, 0, 0)
                kwh = round(random.uniform(0.05, 3.5), 4)
                interval_rows.append(
                    {
                        "meter_id": mid,
                        "read_ts": read_ts.isoformat(sep=" "),
                        "kwh": kwh,
                        "read_date": day_str,
                    }
                )
                daily[(mid, day_str)] = round(daily.get((mid, day_str), 0.0) + kwh, 4)

    # Curated fact = exact daily rollup of the interval rows, with FK keys.
    keys = {m["meter_id"]: m for m in meters}
    fact_rows: list[dict] = []
    for (mid, day_str), kwh in sorted(daily.items()):
        m = keys[mid]
        fact_rows.append(
            {
                "account_id": m["account_id"],
                "meter_id": mid,
                "premise_id": m["premise_id"],
                "day": day_str,
                "kwh": round(kwh, 4),
            }
        )

    return interval_rows, fact_rows


def _write_csv(path: str, fieldnames: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    interval_rows, fact_rows = _gen()

    _write_csv(
        os.path.join(DATA_DIR, "interval_reads.csv"),
        ["meter_id", "read_ts", "kwh", "read_date"],
        interval_rows,
    )
    _write_csv(
        os.path.join(DATA_DIR, "fact_consumption.csv"),
        ["account_id", "meter_id", "premise_id", "day", "kwh"],
        fact_rows,
    )

    print(f"wrote {len(interval_rows)} rows -> data/interval_reads.csv (meter_raw.interval_reads)")
    print(f"wrote {len(fact_rows)} rows -> data/fact_consumption.csv (meter_cur.fact_consumption)")


if __name__ == "__main__":
    main()
