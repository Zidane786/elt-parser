# etl-parser: Deterministic Lineage Engine and Code-Aware Description Generator

Design spec, 2026-09-15. Status: draft for review.

## 1. Purpose

`etl-parser` is a reusable Python package that scans an ETL repository and produces
table-level and column-level data lineage by static analysis of SQL, Python/Pandas,
AWS Lambda, and PySpark code. It then optionally generates column descriptions with an
LLM, grounded in the exact transformation code the lineage engine found.

It exists because:

- Collibra only mirrors schemas from AWS. It cannot read the Git repositories that move data.
- The user's data agent reads a `catalog.json` whose `scripts` section (what each job
  reads and writes) is written by hand. That makes onboarding a data product slow and the
  result drifts from the code.
- Impact assessment across data products needs a graph that connects every product
  through the datasets it consumes and produces.

The package will be published on GitHub for other teams to use.

## 2. Goals and non-goals

Goals:

1. Deterministic lineage. No LLM anywhere in the lineage path.
2. Column-level edges with the transformation expression, source file, and line range on
   every edge, plus a parser name and a confidence level.
3. Unresolved items are first-class output. Dynamic SQL, table names from environment
   variables, and unsupported syntax are reported with a reason, never guessed.
4. Emit three output formats from one internal graph: native lineage JSON, the agent's
   `catalog.json` format, and OpenLineage events for Collibra.
5. Impact assessment: from any column or table, list every downstream dataset, job, and
   product, and every upstream source.
6. Product-to-product dependency graph derived from lineage, checked against the
   dependencies each product declares in `product.yaml`.
7. Pluggable LLM client for descriptions. No provider SDK dependency.

Non-goals for the first release:

- A web UI or REST API. The package is a library plus CLI. An API layer can wrap it later.
- A persistent store. Output is files. A Postgres or graph store can be added behind the
  same models later.
- Runtime PySpark listeners. The package will ingest OpenLineage events produced by the
  OpenLineage Spark listener, but attaching the listener to jobs is a deployment task.
- Executing user code. Static analysis only.

## 3. Consumers and their contracts

### 3.1 The data agent (`de_agent`)

Reads `catalog.json` with top-level `databases`, `relations`, and `scripts`. The package must
emit that exact shape so the agent works unchanged. The exporter fills:

- `databases[].tables[].schema[]` from the Glue Data Catalog or a supplied schema file. The
  flags `to_tokenize`, `verify`, `is_categorical`, `categorical_values`, `is_partition`,
  `nested_keys` are preserved when a prior catalog is supplied, since they are not derivable
  from code.
- `scripts[]` from parsed jobs: `script_name`, `script_path`, `language`, `reads_from`,
  `writes_to`, and `depends_on`. `depends_on` is the union of two sources: data-derived
  (job B reads a dataset job A writes) and orchestrator-declared (B's task lists A's task
  upstream in a DAG). The extension field `depends_on_detail` records for each entry
  whether it came from `data`, `dag`, or `both`, and the datasets that link them. A job that
  reads and writes the same table in place (an SCD merge, a PII purge) is not a dependency
  of the other jobs touching that table by data alone; it is listed only when a DAG declares
  the order, and the builder flags it as `in_place_writer`. `schedule` is the `interval_text` of the job's attached
  `Schedule` (section 6), so the agent keeps seeing a string like `"0 4 * * *"`. A
  top-level `schedules` dict in the extension fields carries the full structured entries.
  `description` is left empty for a human or the AI step to fill.
- `relations[]` is passed through from a prior catalog. Referential relations are a data
  model concept, not lineage, and the package does not infer them.

Extension fields added to the agent format, all optional and ignored by the current agent:
`product` and `layer` on each database, `dataset_id` on each table, `description_source`
(`human`, `ai`, `verified`) on each description, a top-level `schedules` dict, and a
top-level `lineage` block containing column edges and unresolved items.

### 3.2 Impact assessment

A Python API and CLI command that takes a dataset or column identifier and returns the
downstream closure: datasets, columns, jobs, and products, grouped by distance. The same
walk upstream gives provenance. Cross-product edges are marked.

### 3.3 Collibra and other catalogs

OpenLineage `RunEvent` JSON files, one per job, with `ColumnLineageDatasetFacet` on each
output dataset. Collibra reads these from a cloud bucket through its Edge component.
DataHub and Marquez accept the same events over HTTP.

## 4. Architecture

```
repo path
   │
   ▼
RepoScanner ───────────────► ModuleGraph (imports resolved to files)
   │
   ├─ .sql files ──────────► SqlWorker (sqlglot)
   ├─ .py files ───────────► PythonWorker (ast + astroid)
   │      ├─ SQL strings ──► SqlWorker
   │      ├─ pandas / boto3 / SQLAlchemy I/O ──► sink table
   │      └─ PySpark DataFrame chains ──► SparkStaticWorker
   ├─ Airflow DAG files ───► AirflowWorker (schedules, task → job, declared order)
   └─ OpenLineage event files (optional) ──► OpenLineageIngest
   │
   ▼
LineageGraph (networkx) ◄── ProductRegistry (product.yaml files)
   │
   ├─► NativeExporter        lineage.json
   ├─► AgentCatalogExporter  catalog.json  (de_agent format)
   ├─► OpenLineageExporter   events/*.json
   ├─► ImpactAnalyzer        downstream / upstream queries
   └─► DescriptionEngine     prompts ──► LLMClient (caller-supplied) ──► descriptions
```

Every worker returns the same `WorkerResult`: datasets, jobs, column edges, table edges,
and unresolved items. The graph builder merges results, unifies dataset identities, and
attaches ownership from the registry.

## 5. Package layout

```
etl_parser/
  __init__.py
  models/            pydantic models (section 6)
  identity.py        dataset id normalization and alias merging
  registry.py        ProductRegistry from product.yaml files
  scanner/
    repo.py          walk repo, build ModuleGraph, route files
    imports.py       resolve absolute and relative imports to file paths
    sql_strings.py   reconstruct SQL literals from f-strings, format, concat
    sinks.py         declarative table of I/O call patterns and their dialects
  workers/
    base.py          Worker protocol and WorkerResult
    sql.py           sqlglot wrapper: normalize statement, qualify, lineage
    python.py        ast visitor for I/O calls and SQL execution sites
    pandas_chain.py  DataFrame variable tracking for column projections
    spark_static.py  PySpark DataFrame chain tracing
    airflow.py       DAG parsing: schedules, operators to jobs, task order
    openlineage_in.py  ingest runtime OpenLineage events
  graph/
    builder.py       merge WorkerResults into LineageGraph
    impact.py        downstream and upstream closures, cross-product edges
    products.py      product dependency graph and drift check vs declared
  export/
    native.py        lineage.json
    agent_catalog.py catalog.json in de_agent format
    openlineage_out.py RunEvent files with column lineage facets
  describe/
    prompt.py        build grounded prompt from edge + schema + upstream docs
    client.py        LLMClient protocol
    engine.py        topological order, batching, unreviewed marking
  cli.py             typer app: scan, export, impact, describe
tests/
  fixtures/          small copies of de_agent data products (bill, meter, reg)
```

## 6. Data model

All models are pydantic v2. Identifiers are strings so they serialize cleanly.

- `DatasetRef`: `id` (canonical, section 7), `namespace`, `name`, `kind`
  (`table`, `s3_path`, `api`, `kafka`, `kinesis`, `file`), `aliases` (other ids that resolve
  to this dataset), `physical_location`, `product` (optional), `layer` (optional).
- `ColumnRef`: `dataset_id`, `name`, `datatype` (optional).
- `Transformation`: `expression` (SQL or Python text), `kind` (`identity`, `expression`,
  `aggregation`, `filter`, `join`, `window`, `unknown`), `source_file`, `line_start`,
  `line_end`.
- `Provenance`: `parser` (`sqlglot`, `python_ast`, `pandas_chain`, `spark_static`,
  `openlineage_runtime`), `confidence` (`exact`, `inferred`, `partial`), `dialect`
  (optional), `scan_commit` (optional).
- `ColumnEdge`: `target: ColumnRef`, `sources: list[ColumnRef]`, `transformation`,
  `provenance`, `job_id`.
- `TableEdge`: `target: DatasetRef id`, `source: DatasetRef id`, `provenance`, `job_id`.
  Used when a job reads or writes a dataset but columns could not be resolved.
- `Job`: `id`, `name`, `source_file`, `language` (`sql`, `python`, `pyspark`), `engine`
  (`athena`, `spark`, `postgres`, `pandas`, `unknown`), `dialect`, `schedule_id` (optional,
  key into `schedules`), `product` (optional), `inputs`, `outputs`.
- `Schedule`: `id` (`<dag_id>.<task_id>`), `orchestrator` (`airflow`, `product_yaml`,
  `cron_comment`), `dag_id`, `task_id`, `cron` (normalized five-field cron or `None`),
  `interval_text` (the raw value: `"@daily"`, `"0 4 * * *"`, `timedelta(hours=1)`,
  timetable class name), `timezone`, `start_date`, `catchup`, `owner`, `tags`,
  `declared_upstream` (task ids), `declared_downstream` (task ids), `source_file`, `line`.
- `LineageDocument.schedules`: `dict[str, Schedule]` keyed by `Schedule.id`. A job that
  appears in several DAGs has several schedules; `Job.schedule_id` points at the one whose
  DAG is the product's primary orchestrator, and the others are reachable through the dict.
- `Unresolved`: `kind` (`dynamic_sql`, `dynamic_table_name`, `unsupported_syntax`,
  `unknown_column`, `missing_schema`, `unresolved_import`, `dynamic_schedule`,
  `external_job`), `source_file`, `line`, `reason`, `partial_text` (optional, the
  reconstructed string with placeholders).
- `Product`: `code`, `name`, `domain`, `owners`, `databases` (name, type, layer),
  `declared_dependencies` (product code, tables), `orchestrator`, `schedules`.
- `WorkerResult`: `datasets`, `jobs`, `column_edges`, `table_edges`, `unresolved`.
- `LineageDocument`: the native export. `version`, `generated_at`, `scan_commit`,
  `products`, `datasets`, `jobs`, `schedules`, `column_edges`, `table_edges`, `unresolved`.

## 7. Dataset identity

The same physical dataset appears under different names in code. Identity rules:

- Canonical id is `scheme://namespace/name`. Schemes: `glue` (Athena and Spark tables in the
  Glue catalog), `s3`, `postgres`, `mysql`, `dynamodb`, `api`, `kafka`, `kinesis`, `file`.
- `glue://bill_cur/fact_invoice` and `s3://bucket/bill/cur/fact_invoice/` are merged when
  the Glue catalog reports that S3 location for that table. Without Glue access they stay
  separate and the builder records a `missing_schema` unresolved item.
- Identifiers are lower-cased for Glue, Athena, and Spark. Postgres identifiers keep case
  only when quoted.
- Two-part names `db.table` resolve to `glue` when the executing engine is Athena or Spark,
  and to the engine scheme otherwise. One-part names resolve against a default database
  supplied on the command line or found in a `USE` statement or SparkSession config.
- Dialect belongs to the job, never the dataset. The sink table maps each call site to a
  dialect: Athena client, PyAthena, and awswrangler mean `trino`; `spark.sql` means `spark`;
  psycopg2 and SQLAlchemy Postgres URLs mean `postgres`; SQLAlchemy URLs are parsed for the
  driver prefix.

## 8. Workers

### 8.1 SqlWorker (sqlglot)

Input: SQL text, dialect, optional schema provider, optional source location. Steps:

1. Parse with `sqlglot.parse(sql, read=dialect)`. Parse errors become `unsupported_syntax`.
2. Normalize each statement. `CREATE TABLE AS`, `INSERT INTO ... SELECT`, `INSERT OVERWRITE`,
   and `MERGE ... WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT` are rewritten to a target
   dataset plus a `SELECT`. Plain `SELECT` has no target and yields inputs only. `CREATE VIEW`
   registers the view as a dataset whose edges point through to its sources.
3. `qualify` with the schema when available so stars expand and unqualified columns resolve.
   Without a schema, stars produce a `TableEdge` and a `missing_schema` item.
4. For each output column call `sqlglot.lineage.lineage`. Walk the returned nodes to leaf
   table columns. The expression of the projection becomes `Transformation.expression`. The
   kind is `aggregation` if the projection contains an aggregate function, `window` for
   window functions, `identity` if the projection is a bare column reference, else
   `expression`.
5. Columns referenced only in `WHERE`, `JOIN ON`, `GROUP BY`, or `HAVING` are recorded as
   indirect sources on every output column with kind `filter`, `join`, or `aggregation`,
   matching OpenLineage indirect transformation types.
6. Multi-statement scripts keep a session table map so a temp table created in statement 1
   is expanded when read in statement 3.

A `SchemaProvider` protocol has two implementations: a dict loaded from an existing
`catalog.json`, and a Glue Data Catalog reader using boto3.

### 8.2 PythonWorker (ast and astroid)

Input: a `.py` file and the module graph. Steps:

1. Build the module's import map. Local imports resolve to files through the scanner.
   Unresolvable imports become `unresolved_import` items.
2. Visit every `Call`. Match against the sink table by fully qualified callee name. The
   sink table entry says whether the call reads or writes, which argument carries the
   dataset name or SQL, the dataset scheme, and the dialect.
3. For SQL-carrying calls, reconstruct the string with `sql_strings.py`. Literal parts
   join directly. Names resolve through astroid inference to module constants,
   `os.environ.get` defaults, and simple f-string variables. Anything else becomes a
   `{{placeholder}}` and the statement is passed to SqlWorker only if the placeholder is in
   a position that does not affect table or column identity, for example a date filter
   literal. Otherwise the call yields a `dynamic_sql` item with the partial text.
4. Track DataFrame variables. A `read_*` call creates a frame bound to a dataset. Column
   subscripts, `rename`, `assign`, `drop`, `merge`, `groupby().agg`, and `withColumn`-style
   calls create projections. A `to_*` call binds the frame to a target dataset and emits
   column edges for every tracked column. Unknown operations on a frame downgrade all of
   its edges to `partial` confidence and continue rather than stopping.
5. Function calls into helper modules are followed one level when the callee is resolved in
   the module graph and the argument is a tracked frame or a literal. Deeper recursion is
   recorded as `partial`.
6. Lambda handlers: `event["Records"][*]["s3"]["bucket"]["name"]` patterns are recognized as
   an S3 input whose exact key is unknown, recorded as an `s3` dataset with `partial`
   confidence.

Initial sink table coverage: pandas `read_sql`, `read_sql_table`, `read_sql_query`,
`read_csv`, `read_parquet`, `read_json`, `to_sql`, `to_csv`, `to_parquet`; polars `read_*`,
`scan_*`, `write_*`, `sink_*`; boto3 `athena.start_query_execution`, `s3.get_object`,
`s3.put_object`, `s3.upload_file`, `s3.download_file`, `glue.get_table`; awswrangler
`wr.athena.read_sql_query`, `wr.s3.read_parquet`, `wr.s3.to_parquet`; SQLAlchemy
`engine.execute`, `connection.execute`, `text`; psycopg2 `cursor.execute`; PySpark
`spark.sql`, `spark.read.table`, `spark.read.parquet`, `spark.read.format().load()`,
`df.write.saveAsTable`, `df.write.parquet`, `df.write.insertInto`. The table is data, not
code, so teams can extend it.

### 8.3 SparkStaticWorker

Shares the DataFrame tracker with PythonWorker. Handles `select`, `withColumn`,
`withColumnRenamed`, `drop`, `filter`, `where`, `join`, `groupBy().agg`, `alias`, `union`,
and `F.col` / `F.expr` / `F.lit` expressions. `F.expr` strings and `selectExpr` strings go
to sqlglot with dialect `spark`. UDF calls emit an edge with kind `unknown` and confidence
`partial` from all UDF argument columns. Glue `DynamicFrame` `from_catalog` and
`write_dynamic_frame.from_catalog` map to Glue datasets at table level.

### 8.4 AirflowWorker

Input: a `.py` file that imports `airflow` (detected by the scanner). Static only, the DAG
file is never imported or executed. Steps:

1. Find `DAG(...)` constructor calls and `@dag` decorated functions. Read `dag_id`,
   `schedule` or `schedule_interval`, `start_date`, `catchup`, `tags`, and
   `default_args["owner"]`. Values come through the same constant folder as SQL strings, so
   module-level constants and f-strings resolve. `schedule` values are normalized: cron
   strings pass through, presets (`@daily`, `@hourly`, `@weekly`, `@monthly`, `@once`) map
   to cron, `timedelta(...)` is kept as `interval_text` with `cron=None`, and a timetable
   class is kept by name. Anything unresolvable becomes an `Unresolved` item of kind
   `dynamic_schedule` and the schedule is kept with `cron=None`.
2. Find operator instantiations inside the `with DAG(...)` block or the decorated function
   body. Each becomes a task. Operator to job mapping:
   - `BashOperator.bash_command`: extract the script path from `python <path>` or
     `spark-submit ... <path>`; resolve relative to repo root; link to the job the
     PythonWorker produced for that file.
   - `PythonOperator.python_callable`: resolve the function through the module graph to
     its defining file; link to that file's job.
   - `AthenaOperator.query`, `SQLExecuteQueryOperator.sql`, `PostgresOperator.sql`,
     `TrinoOperator.sql`: the SQL is a lineage source in its own right. Pass it to
     SqlWorker with the dialect implied by the operator, and create a job for the task.
   - `GlueJobOperator.job_name`, `EmrAddStepsOperator`, `SparkSubmitOperator.application`:
     link to the script path when given, else record the task with no job and an
     `Unresolved` item of kind `external_job`.
   - `TriggerDagRunOperator` and `ExternalTaskSensor`: record cross-DAG dependencies as
     `declared_upstream` so schedules across products connect.
   - Any other operator: recorded as a task with `job_id=None`.
   - `@task` decorated functions inside a `@dag` are treated like `PythonOperator`.
3. Read dependencies from `a >> b`, `a << b`, `[a, b] >> c`, `set_upstream`,
   `set_downstream`, and `chain(...)`. Fill `declared_upstream` and `declared_downstream`.
4. Emit one `Schedule` per task, and one `Schedule` for the DAG itself with `task_id=None`
   so a DAG with no resolvable tasks still records its cron.

`product.yaml` schedules are read by the registry and become `Schedule` entries with
orchestrator `product_yaml`. A `Schedule:` line in a job's docstring or a SQL header comment
becomes orchestrator `cron_comment`. Precedence when attaching `Job.schedule_id` is Airflow,
then `product.yaml`, then comment.

The graph builder compares each task's `declared_upstream` with the job dependencies
derived from data. A task that runs after another without reading its output, or one that
reads a dataset produced by a task it does not wait for, is reported by the `products`
command as orchestration drift alongside product dependency drift.

### 8.5 OpenLineageIngest

Reads OpenLineage `RunEvent` JSON files produced by the Spark listener. Maps input and
output datasets and the column lineage facet into `ColumnEdge` with parser
`openlineage_runtime` and confidence `exact`. When a runtime edge and a static edge exist
for the same target column, the runtime edge wins and the static edge is kept with a
`superseded_by` note in the native export.

## 9. Graph builder and impact analysis

`LineageGraph` wraps a `networkx.MultiDiGraph`. Nodes are datasets, columns, and jobs.
Edges are `reads`, `writes`, and `derives` (column to column, carrying the `ColumnEdge`).

Build steps: merge worker results, apply identity rules, attach `product` and `layer` from
the registry by matching database names, derive job dependencies from data, merge in
DAG-declared task order as a second dependency source, derive product dependencies,
compute drift between derived and declared product dependencies and between data-derived
and DAG-declared job order.

Job dependency rules:

- `data`: B depends on A when B reads a dataset that A writes, and A is not B.
- `dag`: B depends on A when B's task lists A's task as upstream, directly or through
  `chain()`, `TriggerDagRunOperator`, or `ExternalTaskSensor`.
- A job that both reads and writes the same dataset is an `in_place_writer` for it. Other
  readers of that dataset do not get a `data` dependency on it, because static analysis
  cannot tell whether it runs before or after them. Only a `dag` edge can order it.
- The final `depends_on` is the union. Each entry keeps its source set so the agent
  catalog exporter and the `products` command can show which dependencies are declared,
  which are observed, and which are both.

Impact API:

- `downstream(node_id, max_depth=None) -> ImpactReport` with datasets, columns, jobs, and
  products grouped by hop distance, cross-product edges flagged.
- `upstream(node_id, max_depth=None) -> ImpactReport`.
- `product_graph() -> list[ProductDependency]` with `derived`, `declared`, and
  `status` in `confirmed`, `undeclared`, `declared_but_unobserved`.

## 10. Exporters

- `NativeExporter` writes `LineageDocument` as `lineage.json`.
- `AgentCatalogExporter` writes `catalog.json` per section 3.1. Accepts an optional prior
  catalog to preserve descriptions, flags, and relations. Tables the scan found but the
  prior catalog lacks are added with empty descriptions. Tables in the prior catalog the scan
  never touched are kept unchanged.
- `OpenLineageExporter` writes one `RunEvent` per job with `eventType: COMPLETE`, a
  synthetic run id derived from job id and scan commit, `SchemaDatasetFacet` on every
  dataset with a known schema, and `ColumnLineageDatasetFacet` on outputs. Transformation
  kinds map to `DIRECT` with subtype `IDENTITY`, `TRANSFORMATION`, or `AGGREGATION`, and to
  `INDIRECT` with subtype `FILTER`, `JOIN`, `GROUP_BY`, or `WINDOW`. Uses the
  `openlineage-python` generated classes for serialization and validation.

## 11. Description engine

Deterministic parts live in the package. The LLM call does not.

- `LLMClient` protocol: `complete(system: str, user: str) -> str`. The caller passes any
  object with that method. A Bedrock example using boto3 and a stub for tests ship in
  `describe/client.py`. No provider SDK is a dependency.
- `PromptBuilder` takes a target column, its `ColumnEdge` list, the source column
  datatypes and existing descriptions, the table description, and the product domain. It
  produces a system prompt that demands JSON with `description`, `business_rule`, and
  `confidence`, and a user prompt containing the transformation expression verbatim, the
  source columns with their descriptions, and the dialect. Columns with only `identity`
  edges inherit the upstream description and are not sent to the LLM unless forced.
- `DescriptionEngine` orders tables topologically so upstream descriptions exist before
  downstream prompts are built, batches columns per table, and writes results with
  `description_source: ai`. Nothing is marked `verified` by the package. The agent's
  existing human-in-the-loop review flow flips that flag.

## 12. CLI

```
etl-parser scan <repo> [--products <dir>] [--schema catalog.json | --glue] [--default-db X]
                       [--openlineage-events <dir>] [--out lineage.json]
etl-parser export catalog <lineage.json> [--prior catalog.json] --out catalog.json
etl-parser export openlineage <lineage.json> --out events/
etl-parser impact <lineage.json> <dataset_or_column_id> [--upstream] [--depth N]
etl-parser products <lineage.json>            # derived vs declared dependency table
etl-parser describe <lineage.json> --catalog catalog.json --client <module:factory>
```

Exit code is non-zero if any `unresolved` item has kind `unsupported_syntax`, so CI can
gate on parser coverage. Other unresolved kinds are reported but do not fail.

## 13. Error handling and confidence policy

- A worker never raises on user code. Every failure becomes an `Unresolved` item with file
  and line, and the worker continues with the next call or statement.
- Confidence `exact` means every source column was resolved through a parsed expression
  or a runtime plan. `inferred` means a heuristic was applied, for example a helper function
  followed one level or a DataFrame operation the tracker models approximately. `partial`
  means some sources are known to be missing.
- The native export always includes counts of edges by confidence and unresolved items by
  kind, so a scan's coverage is visible at a glance.

## 14. Testing

- Unit tests per worker with inline SQL and Python snippets covering CTEs, joins, stars
  with and without schema, `INSERT SELECT`, `CTAS`, `MERGE`, f-string SQL with resolvable and
  unresolvable names, pandas read-transform-write, PySpark select and withColumn chains,
  Lambda S3 event handlers, and Airflow DAGs using the context manager, the `@dag`
  decorator, presets, timedelta schedules, `>>` chains, `chain()`, and `AthenaOperator` SQL.
- Fixture tests against copies of three data products from `de_agent` (`bill`, `meter`,
  `reg`) placed under `tests/fixtures`. Expected `lineage.json` and `catalog.json` are
  committed as golden files. `reg` exercises cross-product joins between `bill_cur` and
  `meter_cur`, so it is the impact assessment test.
- The primary PySpark fixture is a copy of `de_agent/test_data/etl` (28 DataFrame API
  scripts, one `spark.sql` f-string, two S3 parquet reads) with its `catalog.json` as the
  expected `scripts` output. A 2026-09-15 spike with a 150-line ast script already matched
  writes 28/28 and reads 23/28; the five read differences were tables the scripts read but
  the hand-written catalog omitted, so the golden file is corrected to the code before
  being committed. The same corpus is the coverage target for `SparkStaticWorker`: the
  DataFrame methods it must model are `select`, `alias`, `withColumn`, `withColumnRenamed`,
  `filter`, `join`, `groupBy().agg`, `unionByName`, `dropDuplicates`, `drop`, `distinct`,
  `orderBy`, `cast`, `over`, `when().otherwise()`, and 37 distinct `F.*` functions.
- An OpenLineage export test validates every event against the facet schemas bundled with
  `openlineage-python`.
- Property: re-running a scan on the same commit produces byte-identical `lineage.json`.
  Sorting is applied everywhere before writing.

## 15. Dependencies

Runtime: `pydantic>=2`, `sqlglot>=30`, `grimp`, `libcst`, `astroid`, `openlineage-python`,
`networkx`, `pyyaml`, `typer`. Optional extras: `glue` adds `boto3`; `spark-exec` adds
`sqlframe` for a future executing PySpark path.

Development: `pytest`, `ruff`, `hatchling` as build backend, `uv` for resolution against
Nexus. `requires-python >= 3.11` to match the existing plugin.

Not used: `anthropic`, `sqllineage`, `sqlglotc`, `jedi`. Reasons are recorded in
`docs/dependencies.md`.

## 16. Implementation order

Matches the five agreed steps, with the scanner split so each step has a testable output.

1. Models, identity rules, product registry, native exporter, package skeleton, CI lint and
   test.
2. SqlWorker with schema provider and golden tests on the `reg` SQL files.
3. RepoScanner, import resolution, SQL-string reconstruction, sink table, PythonWorker with
   table-level I/O for pandas and PySpark, AirflowWorker with the schedules dict. Golden
   test: `scripts` section of `test_data/etl/catalog.json` reproduced from code, plus `bill`
   jobs and `dag_bill.py`.
4. Graph builder, impact API, product and orchestration drift checks, agent catalog
   exporter, OpenLineage exporter, CLI. Golden test on `bill` + `meter` + `reg` together.
5. SparkStaticWorker column-level tracking over the DataFrame API, with pandas column
   tracking sharing the same tracker. Golden column edges for `stage_orders`,
   `fact_orders`, `mart_customer_ltv`, and `dim_customer_scd2`.
6. Description engine with prompt builder, protocol, Bedrock example, stub client tests.

OpenLineageIngest follows step 5 once real Spark events are available to test against.

## 17. Open questions

1. Package and repository name. `etl-parser` matches the working directory. A name that
   says lineage may be clearer on GitHub.
2. Whether the Glue schema provider should cache to a file so scans run offline after one
   fetch. Recommended yes, reuse the format of `fetch_glue_schema.py`.
3. Whether `relations` (foreign keys) should ever be inferred from join conditions. Not in
   scope now, but the join extraction in SqlWorker makes it cheap later.
