# smart_metering (`meter`)

Standalone data product repo for AMI **smart metering**. It lands 15-minute
interval meter reads from the head-end, cleans them, and rolls them up to a
**daily consumption fact** joined to the meter and account dimensions.

- **Domain:** metering
- **ETL flavor:** PySpark (AWS Glue style), code under `glue/`
- **Orchestrator:** Apache Airflow (`orchestration/dag_meter.py`)
- **Owner:** smart-metering-team@utility.example.com

This folder is a **self-contained repo**: its own `pyproject.toml`, no imports
from sibling product folders or from `data_products/catalog/`. Cross-product
table references are resolved at the data-lake / Athena layer, not in code.

## Repo layout

```
meter/
  pyproject.toml          PEP-621 + uv; pyspark + apache-airflow + pyyaml
  README.md               this file
  .gitignore              ignores __pycache__/.venv/*.parquet; commits data/*.csv
  product.yaml            manifest: databases, owners, cross-product deps, schedules
  glue/
    ingest_interval_reads.py   meter_raw.interval_reads
    stage_reads.py             meter_stg.reads_clean
    build_fact_consumption.py  meter_cur.fact_consumption
  orchestration/
    dag_meter.py          Airflow DAG (one task per glue script, wired by deps)
  data/                   generated FK-consistent seed CSVs (committed)
  gen_data.py             STDLIB-ONLY seed generator
  load_to_athena.py       parquet to S3 + Athena external table DDL (needs AWS creds)
```

## Databases / medallion layers

| Database    | Type   | Layer    | Tables |
|-------------|--------|----------|--------|
| `meter_raw` | Athena | raw      | `interval_reads` |
| `meter_stg` | Athena | staging  | `reads_clean` |
| `meter_cur` | Athena | curated  | `fact_consumption` |

## Cross-product dependencies (tables this product reads)

`build_fact_consumption` reads two curated tables owned by **other** product repos:

- `asset_cur.dim_meter` — from **meter_assets** (`asset`): supplies `account_id` +
  `premise_id` for each `meter_id`.
- `cust_cur.dim_account` — from **customer_master** (`cust`): validates the account.

Shared FK keys (`meter_id`, `account_id`, `premise_id`) are spelled identically
across products so the joins resolve.

## Lineage

```
s3://utility-datalake/meter/landing/interval_reads/   (AMI head-end drop)
        │  ingest_interval_reads  (hourly)
        ▼
meter_raw.interval_reads
        │  stage_reads  (daily 01:00)  — dedup + gap-fill
        ▼
meter_stg.reads_clean
        │  build_fact_consumption  (daily 02:00)
        │      ├── join asset_cur.dim_meter   (meter_id -> account_id, premise_id)
        │      └── join cust_cur.dim_account  (validate account_id)
        ▼
meter_cur.fact_consumption   (grain = meter x day)
```

## How to

### 1. Generate seed data (no deps required)

```bash
python data_products/meter/gen_data.py
```

Writes `data/interval_reads.csv` and `data/fact_consumption.csv`, FK-consistent
with the canonical ID ranges so cross-product joins resolve.

### 2. Load raw tables to Athena (needs AWS creds)

```bash
python data_products/meter/load_to_athena.py
```

Writes parquet to `s3://utility-datalake/meter/raw/<table>/` and registers the
Athena external table. Not run in CI (requires live AWS).

### 3. Run the ETL (PySpark)

```bash
spark-submit glue/ingest_interval_reads.py
spark-submit glue/stage_reads.py
spark-submit glue/build_fact_consumption.py
```

### 4. Run the orchestrator (Airflow)

Drop `orchestration/dag_meter.py` into your Airflow `dags/` folder (or point
`AIRFLOW__CORE__DAGS_FOLDER` at `orchestration/`). The DAG `meter_pipeline`
runs daily at 02:00 and wires the tasks `ingest_interval_reads >> stage_reads
>> build_fact_consumption`.
