# billing (`bill`) data product

Standalone, self-contained repo for the **billing** data product in the utility-sector
fixture corpus. It owns the invoice ledger and re-rates invoices against measured
consumption and tariffs to publish a curated invoice fact.

This repo is fully self-contained: it has its own dependency manifest and does **not**
import from any other product folder or from `data_products/catalog/`. Cross-product
table reads happen at SQL time against the shared warehouse, not via Python imports.

## Repo layout

```
bill/
├── pyproject.toml            # PEP-621 + uv; pandas/sqlalchemy/psycopg2 + airflow + pyyaml
├── product.yaml              # manifest: databases, owners, cross-product deps, schedules
├── README.md
├── .gitignore                # ignores .venv/__pycache__/*.parquet; commits data/*.csv
├── gen_data.py               # stdlib-only fixture generator -> data/*.csv
├── load_to_athena.py         # seed CSV -> parquet on S3 -> Athena external table (needs AWS creds)
├── data/                     # committed fixture seed CSVs (the corpus)
├── jobs/                     # ETL (pandas + JDBC flavor)
│   ├── ingest_invoices.py
│   ├── rate_invoices.py
│   └── build_fact_invoice.py
└── orchestration/
    └── dag_bill.py           # Airflow DAG (one task per job)
```

## Databases & medallion layers

| Database     | Type     | Layer    | Contents |
|--------------|----------|----------|----------|
| `billing_pg` | postgres | source   | Operational invoice ledger (system of record). |
| `bill_raw`   | athena   | raw      | Landed invoices extract. |
| `bill_stg`   | athena   | staging  | Invoices re-rated vs consumption + tariff. |
| `bill_cur`   | athena   | curated  | `fact_invoice` (grain = invoice). |

## Cross-product dependencies

`rate_invoices` reads three curated tables owned by other products:

| Table                       | Owning product           | Key |
|-----------------------------|--------------------------|-----|
| `meter_cur.fact_consumption`| smart_metering (`meter`) | `account_id`, `meter_id` |
| `cust_cur.dim_account`      | customer_master (`cust`) | `account_id` |
| `tariff_cur.dim_tariff`     | tariff_pricing (`tariff`)| `tariff_id` |

FK consistency is guaranteed because `gen_data.py` draws `account_id` and `tariff_id`
from the same canonical id ranges the producing products use.

## How to

Generate fixture data (no deps required):

```bash
python data_products/bill/gen_data.py
```

Install deps and load raw tables to Athena (needs AWS credentials):

```bash
uv sync
uv pip install -e '.[aws]'
python data_products/bill/load_to_athena.py
```

Run the ETL jobs directly (in dependency order):

```bash
python data_products/bill/jobs/ingest_invoices.py
python data_products/bill/jobs/rate_invoices.py
python data_products/bill/jobs/build_fact_invoice.py
```

Run the orchestrator — point Airflow at `orchestration/dag_bill.py`; the DAG runs
`ingest_invoices >> rate_invoices >> build_fact_invoice` on `0 4 * * *`.

## Lineage

```
billing_pg.invoices ──ingest_invoices──> bill_raw.invoices
                                              │
meter_cur.fact_consumption ┐                  │
cust_cur.dim_account       ├──rate_invoices──> bill_stg.invoices_rated
tariff_cur.dim_tariff      ┘                       │
                                  build_fact_invoice──> bill_cur.fact_invoice
```

`fact_invoice` is downstream-consumed by `payments_collections` (`pay`) and
`customer_engagement` (`engage`) via the shared `invoice_id` key.
