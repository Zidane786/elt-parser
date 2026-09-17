# etl-parser Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pip-installable `etl-parser` package that scans an ETL repo, produces a deterministic `lineage.json`, reproduces the de_agent `catalog.json` scripts section from code, answers impact queries, exports OpenLineage events, and builds grounded description prompts.

**Architecture:** Workers (sql, python, airflow) each return a `WorkerResult`; `LineageGraph` merges them with identity rules and the product registry; exporters and the impact analyzer read only the graph. Every failure inside a worker becomes an `Unresolved` record, never an exception.

**Tech Stack:** Python >=3.11, pydantic v2, sqlglot >=30, networkx, pyyaml, typer, openlineage-python (export validation), astroid (constant folding), pytest, ruff, hatchling, uv.

**Spec:** `docs/superpowers/specs/2026-09-15-etl-parser-design.md`

**Implementation status:** This is the original planning checklist, not a live task list.
See `docs/implementation-review.md` for delivered evidence, corrections, and explicit
static-analysis limits. Task 10 below is amended to the requested Agent SDK integration.

**Follow-up proposal (2026-09-17):** [Optional AI lineage, observability, and GitHub
sources](2026-09-17-ai-lineage-observability-github.md) records the subsequent discussion.
It is planned work, not part of the shipped deterministic-only lineage behavior.

## Global Constraints

- No LLM calls outside `describe/`, which uses the private Agent SDK's Lambda Bedrock invoke runner. Lineage is deterministic.
- Workers never raise on user code; emit `Unresolved(kind=...)` and continue.
- Every `ColumnEdge` and `TableEdge` carries `Provenance(parser, confidence, dialect)` and a `Transformation` with `source_file`, `line_start`, `line_end` where known.
- Dialect is decided by the executing call site, never by the table.
- Canonical dataset id is `scheme://namespace/name`, lower-cased for glue/spark/athena.
- Output JSON is sorted and deterministic: same commit in, byte-identical `lineage.json` out.
- Dependencies limited to: pydantic, sqlglot, networkx, pyyaml, typer, openlineage-python, astroid. Optional extra `glue`: boto3.
- `requires-python = ">=3.11"`, build backend hatchling.
- Fixture corpora: `tests/fixtures/etl` (copy of de_agent/test_data/etl + corrected catalog), `tests/fixtures/products/{bill,meter,reg}` (copies of de_agent/data_products).

---

## File Structure

```
pyproject.toml
README.md
etl_parser/__init__.py              version, public API re-exports
etl_parser/models.py                all pydantic models (spec §6)
etl_parser/identity.py              normalize_dataset_id, DatasetRegistry alias merging
etl_parser/registry.py              ProductRegistry from product.yaml
etl_parser/scanner/__init__.py
etl_parser/scanner/repo.py          RepoScanner: walk, classify files, build ModuleGraph
etl_parser/scanner/imports.py       resolve imports to file paths
etl_parser/scanner/strings.py       fold_string: literal reconstruction with placeholders
etl_parser/scanner/sinks.py         SINKS table: callee pattern -> SinkSpec
etl_parser/workers/__init__.py
etl_parser/workers/base.py          Worker protocol helpers, WorkerResult merge
etl_parser/workers/sql.py           SqlWorker
etl_parser/workers/python.py        PythonWorker (table-level I/O, SQL sites, DAG detection hand-off)
etl_parser/workers/airflow.py       AirflowWorker
etl_parser/workers/spark_static.py  SparkStaticWorker (column-level, step 5)
etl_parser/graph/__init__.py
etl_parser/graph/builder.py         build_graph(results, registry) -> LineageGraph
etl_parser/graph/impact.py          downstream/upstream
etl_parser/graph/products.py        product dependency + drift
etl_parser/export/__init__.py
etl_parser/export/native.py         LineageDocument <-> lineage.json
etl_parser/export/agent_catalog.py  catalog.json exporter
etl_parser/export/openlineage_out.py
etl_parser/describe/__init__.py
etl_parser/describe/client.py       lazy Agent SDK BedrockInvokeLambdaRunner factory
etl_parser/describe/prompt.py       PromptBuilder
etl_parser/describe/engine.py       DescriptionEngine
etl_parser/cli.py                   typer app
tests/...                           one test module per source module + fixture tests
```

---

### Task 1: Package skeleton, models, identity

**Files:** `pyproject.toml`, `etl_parser/__init__.py`, `etl_parser/models.py`, `etl_parser/identity.py`, `tests/test_models.py`, `tests/test_identity.py`

**Produces:** every model in spec §6 as pydantic `BaseModel` with `model_config = ConfigDict(extra="forbid")`; enums as `Literal` types. `normalize_dataset_id(name: str, *, engine: str, default_db: str | None) -> str`. `DatasetRegistry.add(ref) / merge_alias(a, b) / resolve(id) -> DatasetRef`.

- [ ] Write `pyproject.toml` (hatchling, deps, ruff, pytest config), `uv sync`.
- [ ] Test: models round-trip through `model_dump_json` and back; `LineageDocument` with sorted lists is stable.
- [ ] Test: `normalize_dataset_id("Ecommerce.Raw_Orders", engine="spark")` -> `glue://ecommerce/raw_orders`; `("orders", engine="athena", default_db="analytics")` -> `glue://analytics/orders`; `("s3://b/p/", engine="spark")` -> `s3://b/p/`; `("billing_pg.invoices", engine="postgres")` -> `postgres://billing_pg/invoices`; `("public.t", engine="postgres")` keeps case only if quoted.
- [ ] Implement, run, commit `feat: models and dataset identity`.

### Task 2: String folding and sink table

**Files:** `etl_parser/scanner/strings.py`, `etl_parser/scanner/sinks.py`, tests.

**Produces:** `fold_string(node: ast.AST, env: Mapping[str, str]) -> Folded(text: str, complete: bool, placeholders: list[str])`; `collect_constants(tree) -> dict[str, str]` (module-level `NAME = <foldable>` and `os.environ.get("X", "default")` -> default). `SinkSpec(callee: str, direction: Literal["read","write","sql"], arg: int | str, scheme: str, dialect: str | None, engine: str)`; `match_sink(qualified_callee: str) -> SinkSpec | None` with suffix matching (`spark.table`, `.read.parquet`, `.write.saveAsTable`, `pd.read_sql`, `to_sql`, `start_query_execution`, `cursor.execute`, `engine.execute`, `wr.athena.read_sql_query`, `wr.s3.to_parquet`, `pl.read_parquet`, `write_parquet`).

- [ ] Tests: f-string with known and unknown names, `%` and `.format`, `+` concat, `os.environ.get` default, nested.
- [ ] Tests: each sink pattern resolves; unknown returns None.
- [ ] Implement, commit `feat: string folding and sink table`.

### Task 3: SqlWorker

**Files:** `etl_parser/workers/base.py`, `etl_parser/workers/sql.py`, `tests/test_sql_worker.py`

**Produces:** `SchemaProvider` protocol `columns(dataset_id) -> list[str] | None`; `DictSchemaProvider(catalog_json_path_or_dict)`; `SqlWorker(schema: SchemaProvider | None).analyze(sql: str, *, dialect: str, engine: str, default_db: str | None, source_file: str | None, line_offset: int, job_id: str) -> WorkerResult`.

Behaviour: parse all statements; normalize CTAS/INSERT/MERGE to (target, select); qualify with schema; per output column `sqlglot.lineage.lineage`; indirect columns from WHERE/JOIN/GROUP BY; CTE names excluded from datasets; session temp-table map across statements; errors -> `Unresolved(unsupported_syntax)`; star without schema -> `TableEdge` + `Unresolved(missing_schema)`.

- [ ] Tests: simple select; CTE + join (the `reg/sql/mart_energy_sold.sql` CTAS with Trino `date_format || CAST`); INSERT ... SELECT; MERGE; `SELECT *` with and without schema; unparseable text; two-statement temp table.
- [ ] Implement, commit `feat: sqlglot lineage worker`.

### Task 4: Repo scanner, imports, PythonWorker (table-level)

**Files:** `etl_parser/scanner/repo.py`, `etl_parser/scanner/imports.py`, `etl_parser/workers/python.py`, tests + `tests/fixtures/etl` (copy 28 scripts + corrected `catalog.json`).

**Produces:** `RepoScanner(root).scan() -> ScanIndex(py_files, sql_files, dag_files, module_map: dict[module_name, Path])`; `resolve_import(module: str, level: int, from_file: Path, index) -> Path | None`; `PythonWorker(sql_worker, index).analyze_file(path) -> WorkerResult` producing one `Job` per script (id = repo-relative path without `.py`), `inputs`/`outputs` dataset ids, `TableEdge`s, `Unresolved`, docstring header parse for `Schedule:` (into a `Schedule` with orchestrator `cron_comment`) and `Owner:`.

- [ ] Test: scanner classifies `dag_bill.py` as dag (imports airflow), sql files, py files; module_map has `jobs.ingest_invoices`.
- [ ] Test: fixture ETL corpus -> reads/writes equal corrected catalog for all 28.
- [ ] Test: helper module followed one level (`from utils import load` where `load` calls `spark.table`).
- [ ] Implement, commit `feat: repo scanner and python table-level worker`.

### Task 5: AirflowWorker

**Files:** `etl_parser/workers/airflow.py`, `tests/test_airflow_worker.py`, fixture `tests/fixtures/products/bill`.

**Produces:** `AirflowWorker(index).analyze_file(path) -> WorkerResult` with `schedules: dict[str, Schedule]` on `WorkerResult` (add field) and `task_jobs: dict[schedule_id, job_id | None]`.

- [ ] Tests: `with DAG(...)` + BashOperator `python path` -> job link + cron; `@dag/@task`; presets -> cron; timedelta -> cron None; `>>`, `[a,b] >> c`, `chain()`; `AthenaOperator(query=...)` -> SqlWorker invoked with trino; unknown operator -> task with `job_id=None`; dynamic schedule -> `Unresolved(dynamic_schedule)`.
- [ ] Implement, commit `feat: airflow dag worker with schedules`.

### Task 6: ProductRegistry and graph builder

**Files:** `etl_parser/registry.py`, `etl_parser/graph/builder.py`, tests.

**Produces:** `ProductRegistry.load(dir_or_files) -> ProductRegistry`; `.product_for_database(db) -> Product | None`; `.layer_for_database(db)`; `.schedules() -> dict[str, Schedule]`. `build_graph(results: list[WorkerResult], registry: ProductRegistry | None, scan_commit: str | None) -> LineageGraph`. `LineageGraph.to_document() -> LineageDocument`; `job_dependencies() -> dict[job_id, list[JobDependency(job_id, sources: set[Literal["data","dag"]], via_datasets, in_place_writer)]]`.

- [ ] Tests: registry from `bill/product.yaml`; in-place writer rule using `purge_pii_after_retention`; data + dag union; product attached by database name (`bill_cur` -> bill).
- [ ] Implement, commit `feat: registry and graph builder`.

### Task 7: Impact and product drift

**Files:** `etl_parser/graph/impact.py`, `etl_parser/graph/products.py`, tests.

**Produces:** `downstream(graph, node_id, max_depth=None) -> ImpactReport(by_hop: list[Hop(datasets, columns, jobs, products)], cross_product_edges)`; `upstream(...)`; `product_dependencies(graph, registry) -> list[ProductDependency(from_product, to_product, datasets, status: confirmed|undeclared|declared_but_unobserved)]`; `orchestration_drift(graph) -> list[Drift]`.

- [ ] Tests on bill+meter+reg fixtures: `glue://meter_cur/fact_consumption` downstream reaches `reg_cur.mart_energy_sold` at hop 1 with cross-product flag; declared deps in bill yaml match observed.
- [ ] Implement, commit `feat: impact analysis and product drift`.

### Task 8: Exporters and CLI

**Files:** `etl_parser/export/native.py`, `agent_catalog.py`, `openlineage_out.py`, `etl_parser/cli.py`, tests.

**Produces:** `write_native(doc, path)`, `read_native(path)`; `export_agent_catalog(doc, prior: dict | None) -> dict`; `export_openlineage(doc) -> list[dict]` validated with openlineage-python classes; typer commands `scan`, `export catalog`, `export openlineage`, `impact`, `products`.

- [ ] Tests: native round-trip byte-identical on second run; agent catalog for ETL fixture equals corrected golden `scripts` (order, fields, `schedule` string, `depends_on`); prior catalog preserves descriptions/flags/relations; OpenLineage events construct without validation error and contain `columnLineage` facet.
- [ ] Implement, commit `feat: exporters and cli`.

### Task 9: SparkStaticWorker column-level

**Files:** `etl_parser/workers/spark_static.py`, tests.

**Produces:** `SparkStaticWorker(sql_worker).analyze_file(path) -> WorkerResult` with `ColumnEdge`s. Frame model: `Frame(columns: dict[out_col, ColExpr(sources: set[(dataset_id, col)], text, kind)], sources: set[dataset_id], alias: str | None, confidence)`. Handles `spark.table/read.*`, `select` (str, `F.col`, `alias`, arithmetic, `cast`, `F.when/otherwise`, `F.coalesce`, aggregate functions -> kind aggregation, `over` -> window), `withColumn`, `withColumnRenamed`, `drop`, `filter/where` (indirect), `join` (alias-qualified `o.col`, `on` string or list, indirect sources), `groupBy().agg`, `unionByName/union`, `dropDuplicates/distinct/orderBy` (pass-through), `selectExpr`/`F.expr` via SqlWorker (spark dialect), `write.saveAsTable/insertInto/parquet`. Unknown method -> pass-through with confidence `partial`. Alias resolution: `F.col("o.x")` resolves through `.alias("o")` on the frame.

- [ ] Tests: `stage_orders` -> `total_usd_cents` sources {raw_orders.total_cents, dim_currency.usd_fx_rate}, kind expression, text contains `coalesce`; `fact_orders` identity edges + `currency` from `original_currency`; `mart_customer_ltv` aggregation kinds and join indirect; `dim_customer_scd2` unionByName merges three branches; unknown method degrades confidence.
- [ ] Implement, commit `feat: static pyspark column lineage`.

### Task 10: Description engine

**Files:** `etl_parser/describe/client.py`, `prompt.py`, `engine.py`, tests.

**Produces (amended):** A lazy factory for the private Agent SDK's `BedrockInvokeLambdaRunner`; no custom LLM protocol or direct provider transport. `build_prompt(...) -> Prompt(system, user)`; `DescriptionEngine(runner, model=...).run(doc, catalog) -> dict` and async `arun(...)` use SDK `Message`/`LLMResponse` types. Only descriptions are enriched, with `description_source="ai"`; identity columns inherit upstream. Tests use SDK `FakeLLMRunner` and mocked Lambda transport through the real SDK.

- [ ] Tests: grounded prompts; identity inheritance; dependency order; SDK JSON responses; malformed/truncated/blocked/tool responses skipped; both Lambda envelopes; scan without SDK/AWS imports.
- [ ] Implement, commit `feat: grounded description engine`.

### Task 11: README, fixture golden files, end-to-end

- [ ] `README.md`: install (uv, Nexus note), CLI usage, output formats, extending the sink table, configuring the Agent SDK Lambda runner.
- [ ] End-to-end test: `scan tests/fixtures/etl` -> `export catalog` equals golden.
- [ ] `ruff check`, `pytest` green, commit `docs: readme and e2e`.

## Self-review

Spec coverage: §3.1 Task 8; §3.2 Task 7; §3.3 Task 8; §6 Task 1; §7 Task 1; §8.1 Task 3; §8.2 Task 4; §8.3 Task 9; §8.4 Task 5; §8.5 OpenLineageIngest deferred per spec §16; §9 Tasks 6–7; §10 Task 8; §11 Task 10; §12 Task 8; §13 all tasks; §14 fixtures Tasks 4, 5, 7, 11.
