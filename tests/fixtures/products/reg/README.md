# regulatory_reporting (`reg`)

PySpark + SQL data product that produces the utility's **regulatory marts**:
reliability indices (SAIDI/SAIFI), energy-sold filings, and a collections
summary. It is a pure **fan-in** product — it owns no shared id space and reads
curated facts from three upstream products.

This folder is a **self-contained repo**: it carries its own `pyproject.toml`,
never imports from sibling product folders or from `data_products/catalog/`, and
its `gen_data.py` is stdlib-only so it runs without installing PySpark/Airflow.

## Repo layout

```
reg/
  pyproject.toml      PEP-621 + uv; deps: pyspark, apache-airflow, pyyaml
  README.md           this file
  .gitignore          python + spark artifacts (data/*.csv kept)
  product.yaml        manifest: domain, owners, databases, deps, schedules
  glue/               PySpark ETLs
    stage_reliability_events.py
    mart_collections_summary.py
  sql/                Athena SQL transforms (scanned too)
    mart_saidi_saifi.sql
    mart_energy_sold.sql
  orchestration/
    dag_reg.py        Airflow DAG (one task per ETL script)
  data/               generated seed CSVs (FK-consistent)
  gen_data.py         STDLIB-ONLY seed generator -> data/*.csv
  load_to_athena.py   wr.s3.to_parquet + CREATE EXTERNAL TABLE per raw table
```

## Databases & medallion layers

| Database  | Type   | Layer    | Contents |
|-----------|--------|----------|----------|
| `reg_raw` | athena | raw      | `reporting_calendar` — regulatory reporting periods |
| `reg_stg` | athena | staging  | `reliability_events` — conformed outage durations per feeder/period |
| `reg_cur` | athena | curated  | `mart_saidi_saifi`, `mart_energy_sold`, `mart_collections_summary` |

## Cross-product dependencies (reads)

This product reads curated facts owned by other repos. Shared key columns are
spelled identically everywhere so the joins resolve:

| Upstream product       | Table read                  | Used for |
|------------------------|-----------------------------|----------|
| outage_management      | `outage_cur.fact_outage`    | reliability events + collections credits |
| billing                | `bill_cur.fact_invoice`     | energy revenue + collections |
| smart_metering         | `meter_cur.fact_consumption`| energy sold (kWh) |

## Lineage

```
outage_cur.fact_outage ──► reg_stg.reliability_events ──► reg_cur.mart_saidi_saifi
                       └──────────────────────────────┐
bill_cur.fact_invoice ─────────────────────────────┐  ├──► reg_cur.mart_collections_summary
meter_cur.fact_consumption ─┐                       │
                            └──► reg_cur.mart_energy_sold
```

- `stage_reliability_events` conforms `outage_cur.fact_outage` into per-feeder/period
  reliability inputs (`reg_stg.reliability_events`).
- `mart_saidi_saifi` (SQL CTAS) computes SAIDI/SAIFI from the staged events.
- `mart_energy_sold` (SQL CTAS) joins `bill_cur.fact_invoice` to
  `meter_cur.fact_consumption` on `account_id` for energy + revenue per period.
- `mart_collections_summary` (PySpark) joins invoices to outage credits per
  period and writes the collections summary.

## How to

### Generate seed data (no deps required)
```
python gen_data.py
```
Writes FK-consistent CSVs into `data/`.

### Load raw tables to Athena (needs AWS creds)
```
uv sync --extra load
AWS_PROFILE=... python load_to_athena.py
```
Writes parquet to `s3://utility-datalake/reg/raw/<table>/` and registers the
Athena external tables.

### Run the ETL
PySpark steps:
```
spark-submit glue/stage_reliability_events.py
spark-submit glue/mart_collections_summary.py
```
SQL marts (Athena CTAS):
```
aws athena start-query-execution \
  --query-string "$(cat sql/mart_saidi_saifi.sql)" \
  --query-execution-context Database=reg_cur
aws athena start-query-execution \
  --query-string "$(cat sql/mart_energy_sold.sql)" \
  --query-execution-context Database=reg_cur
```

### Run the orchestrator (Airflow)
`orchestration/dag_reg.py` defines the `reg_pipeline` DAG (schedule
`0 4 1 * *`). Place it on the Airflow `dags/` path; tasks are wired as
`stage_reliability_events >> mart_saidi_saifi`, with `mart_energy_sold` and
`mart_collections_summary` as independent fan-in tasks.
