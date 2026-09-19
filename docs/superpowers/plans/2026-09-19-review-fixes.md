# Review fixes and AI confidence — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close every finding in `docs/reports/2026-09-19-full-review.md`, add model-reported
`ai_confidence` to every AI-generated edge, dataset, job and description, and infer
foreign-key relations from join conditions as a separately marked, optional output.

**Architecture:** No new modules. Each work package touches a disjoint set of files so
packages run in parallel. Shared model changes (`models.py`: `Origin`, `ai_confidence`,
`ai_rationale`, `ai_inputs`/`ai_outputs`, `analysis_note`, `skipped_entry`) are already
committed at `8e52ba6` and are the base for every package.

**Tech Stack:** unchanged (pydantic, sqlglot, networkx, pyyaml, typer, openlineage-python).
`astroid` is removed.

**Spec:** `docs/superpowers/specs/2026-09-15-etl-parser-design.md`,
`docs/superpowers/specs/2026-09-17-ai-lineage-observability-github-design.md`,
`docs/reports/2026-09-19-full-review.md` (finding numbers below refer to it).

## Global Constraints

- Workers never raise on user code; every dropped construct becomes an `Unresolved`.
- Deterministic edges/diagnostics are never removed or altered by AI merge.
- Every AI-produced item carries `parser=agent_sdk_ai`, `confidence=inferred`, `model_id`,
  `request_id`, `evidence_digest`, `ai_confidence` (0-1), `ai_rationale`.
- Output stays byte-identical across runs; sort every set before it reaches a list.
- Tests: TDD per fix; the full suite plus `ruff check` and `ruff format --check` must pass.
- Golden `tests/fixtures/catalog_scripts_golden.json` may only be regenerated in WP-B, with
  the diff explained in the commit message.
- Docs (`README.md`, `docs/*.md`) are edited only in WP-F.
- Nothing provider-specific (gateway URLs, keys, vendor names) is written anywhere.

## Work packages

### WP-A Catalog export (findings 1, 2, description_source, relations, source-of-truth schema)

Design decision (user, 2026-09-19): the source system (Glue, Postgres, Redshift, or a schema
file produced from them) is the truth for databases, tables, columns, datatypes and partition
keys. Code tells us only how data is used. Therefore, by default, tables and columns that are
discovered only in code are NOT added to the `databases` section. A table referenced in code
but absent from the source schema becomes `Unresolved(kind="missing_in_source")` (non-gating).
`export_agent_catalog(..., include_code_schema=False)` and CLI `--schema-from-code` restore the
old behaviour of adding code-derived tables/columns (marked `schema_source: code`).

Files: `etl_parser/export/agent_catalog.py`, `tests/test_agent_catalog.py` (new).

- [ ] Match prior databases by `db_name` first. If exactly one prior database has that name,
  merge into it regardless of `db_type`; if several, prefer the same scheme; never emit
  `db_type: "glue"` for new databases (use `athena`). Test with
  `tests/fixtures/etl_catalog_original.json` as prior: 2 databases, all 946 prior fields kept.
- [ ] Match prior scripts by `job_id`, then exact `script_path`, then `script_name`, then
  path suffix. Keep unmatched prior scripts unchanged at the end of `scripts`.
- [ ] Every description gets `description_source` (`human` for prior text without a source,
  `inherited`, `ai`, `code` for a job docstring first line). Script `description` falls back
  to the job docstring first line with `description_source: code`.
- [ ] AI descriptions carry `ai_confidence`, `ai_rationale`, `ai_model` when present on the
  incoming catalog column (populated by WP-E).
- [ ] `relations_inferred` (new top-level list): from `ColumnEdge.indirect_sources` with kind
  `join` or from `doc.join_conditions` if WP-D provides it, emit
  `{from_table, to_table, from_column, to_column, relation_type: "join", source: "inferred",
  jobs: [...]}`. Never merge into `relations`.
- [ ] Table-level `dataset_id`, `product`, `layer` unchanged.
- [ ] Default `include_code_schema=False`: only tables/columns present in the supplied schema
  (or prior) appear under `databases`; code-only tables produce `missing_in_source` items
  (emit them from the exporter into `doc`-independent `catalog["lineage"]["unresolved"]` and
  return them so the CLI can log them). Source-provided `relations` (from WP-G) are written to
  `relations` with `source: "database"`; prior human relations are kept.
- [ ] `catalog["schema_drift"]` (new top-level dict, user request 2026-09-19):
  `{"code_only": {"databases": [...]}, "unused_in_code": [{"db_name", "table_name"}]}`.
  `code_only.databases` uses the exact `databases` shape (`db_name`, `db_type`, `tables[]
  {table_name, description: "", schema[] {field_name, datatype: null, description: ""},
  referenced_by: [job ids], source_files: [...]}`) so an entry can be copied into the real
  catalog once verified against the source system. `unused_in_code` lists source tables no
  scanned job reads or writes. Both lists are sorted. Also emitted to the log as one
  `schema.drift` event with counts.

### WP-B Graph, impact, OpenLineage, registry, identity (3, 11, 12, 13, 27–31)

Files: `etl_parser/graph/builder.py`, `etl_parser/graph/impact.py`, `etl_parser/graph/products.py`,
`etl_parser/export/openlineage_out.py`, `etl_parser/registry.py`, `etl_parser/identity.py`,
`tests/test_graph_fixes.py` (new), `tests/fixtures/catalog_scripts_golden.json` (regenerate).

- [ ] Dependency rule: skip an in-place writer only when the dataset has another writer;
  otherwise add the dependency with `in_place_writer=True`. `in_place_writer` computed per
  `via_datasets`. Regenerate golden; expected diff: `mart_cohort_retention` and
  `mart_customer_ltv` gain `dim_customer_scd2`.
- [ ] Add `job_io` fallback edges (job input × output) so impact never misses a declared read;
  mark them `provenance.parser="job_io"`, `confidence="partial"` and exclude them from
  OpenLineage column facets.
- [ ] Column-level impact: map column nodes to datasets before intersecting with table edges;
  dataset-level walk populates `columns`.
- [ ] OpenLineage: INDIRECT subtype from edge kind (`filter`→FILTER, `join`→JOIN,
  `aggregation`→GROUP_BY, `window`→WINDOW); field `transformationType` limited to spec values.
  Fix `PRODUCER` typo (`etl-parser`).
- [ ] `depends_on` uses `job_id` when two jobs share a `script_name`.
- [ ] Product attach honours `ProductDatabase.type` against the dataset scheme.
- [ ] Registry reads both `schedules.steps` and `schedules.scripts`.
- [ ] `normalize_dataset_id("")`/`"."`/no-scheme inputs return an `Unresolved`-friendly
  sentinel instead of raising; add guards at call sites in this package.
- [ ] Conflicting aliases and orchestrator cycles use kind `analysis_note`.

### WP-C Python worker and scanner (4, 8, 17–20, 32–34, 36, 38, inferred confidence)

Files: `etl_parser/workers/python.py`, `etl_parser/scanner/repo.py`, `etl_parser/scanner/sinks.py`,
`etl_parser/scanner/imports.py` (delete or wire in), `tests/test_python_worker_fixes.py` (new).

- [ ] Writer builder fallback: `Frame.write.<chain>.{parquet,csv,json,orc,text,save}` resolves
  like the reader fallback; `.option("path", ...)` tracked on writers.
- [ ] Recursion guard: catch `RecursionError` in `analyze_file`/`evaluate`, emit
  `Unresolved(unsupported_syntax)`; `issue()` must not `ast.unparse` nodes deeper than a limit.
- [ ] `relative_to` guarded; paths outside root use the absolute path string.
- [ ] Reject placeholder/`*`/empty column names → `unknown_column`, never an edge.
- [ ] Evaluate call arguments of unknown calls, `for.iter`, tuple targets, `try` handlers/
  `orelse`/`finally`, `match`, class bodies, `AugAssign`, walrus; nested `return` inside
  `if`/`for`/`with` respected.
- [ ] Emit `confidence="inferred"` for helper-followed frames, branch-merged frames and UDF
  edges (UDF kind `unknown`). Merge `kind` and `indirect` across `unionByName` branches.
- [ ] Language detection from imports, not substring; set `parser="spark_static"` for Spark.
- [ ] Sink guards for bare `execute`/`text` (receiver must be a known connection/cursor).
- [ ] `spark.read.parquet(path=...)` keyword form; `Connection.cursor()` followed for dialect.
- [ ] ZIP: chunked bounded read; local `.zip` size check; one bad member does not abort the rest.
- [ ] Remove dead `resolve_import` or use it; deduplicate `callee()`.
- [ ] Heuristic control-flow notes use kind `analysis_note`.

### WP-D Airflow, SQL, base (5, 6, 7, 14–16, 25, relations source)

Files: `etl_parser/workers/airflow.py`, `etl_parser/workers/sql.py`, `etl_parser/workers/base.py`,
`etl_parser/scanner/strings.py`, `tests/test_airflow_fixes.py`, `tests/test_sql_fixes.py` (new).

- [ ] Constructor DAGs: bind tasks by `dag=` kwarg and `with dag_var:`; scope by binding.
- [ ] Visit `for`/`while`/`try`/comprehensions/`.expand()`/`.partial()`; unresolvable
  task ids → `dynamic_schedule`; TaskGroup prefixes and group-level `>>`; `@task.*` variants;
  task detection by `task_id=` kwarg for custom operators.
- [ ] Bind dependency symbols at assignment time.
- [ ] Jinja: `{{ ... }}` inside string literals becomes a placeholder literal; statement is
  analysed with `confidence="partial"`; report at the hole's line; clean statements kept.
- [ ] Temp tables excluded from `inputs`/`outputs`/`datasets`/`table_edges`; table edges
  re-pointed through.
- [ ] Embedded SQL `line_offset` anchored to the string constant's line (Airflow and the
  hook used by `python.py`, exposed as a helper in `base.py`).
- [ ] `UPDATE ... FROM ... JOIN` indirect sources; duplicate output names → `unknown_column`.
- [ ] Model `MERGE ... DELETE`, `UPDATE SET *`, `INSERT OVERWRITE DIRECTORY`, `SELECT INTO`,
  `CREATE TABLE ... LIKE`, `DELETE ... WHERE IN (subquery)`, `LOCATION 's3://'` as alias
  evidence.
- [ ] `sql=[...]` lists and `.sql` file paths on operators; `parse_header` strips trailing
  `--` comments; 6-field cron → `dynamic_schedule`.
- [ ] Expose join conditions: `SqlAnalysis.join_conditions: list[(left ColumnRef, right
  ColumnRef)]` and propagate via `WorkerResult.join_conditions` (add field to models.py in
  this package only if absent; coordinate) for WP-A relations inference.

### WP-E AI analysis, description engine, sources (9, 10, 21, 22, 24, 37, ai_confidence)

Files: `etl_parser/ai_analysis.py`, `etl_parser/describe/*.py`, `etl_parser/sources.py`,
`tests/test_ai_confidence.py` (new), existing AI tests updated for the new response schema.

- [ ] Response schema: `ColumnProposal`, `TableProposal`, `DescriptionProposal` gain
  `confidence: float (0-1)` and `rationale: str (<=500)`, required. Prompt instructions ask
  for them. Accepted edges set `Provenance.ai_confidence`/`ai_rationale`; catalog columns get
  `ai_confidence`, `ai_rationale`, `ai_model`, `description_source: ai`.
- [ ] Optional `AnalysisConfig.min_ai_confidence` (default 0.0): proposals below it are
  recorded as `deferred` with reason `below_confidence_threshold`. Exposed on `ParserClient`
  through `AnalysisConfig` and on the CLI as `--min-ai-confidence` (WP-F documents it).
- [ ] AI-introduced datasets get `origin="ai"` and `provenance`; jobs get `ai_inputs`/
  `ai_outputs` instead of mutating `inputs`/`outputs`; a job created from AI only gets
  `origin="ai"`. `changes.json` records dataset/job additions.
- [ ] Evidence: quote ≥ 12 characters and must contain the target column name or an
  identifier from the expression; dataset validation by identifier token match.
- [ ] Pre-flight provider configuration once before the file loop; raise
  `AnalysisPolicyError("provider_configuration_invalid")`; split `except Exception` so
  non-provider errors carry reason `internal_error`.
- [ ] Legacy `describe` prompt: drop `provenance`/`job_id` from the payload, pass `domain`
  and source datatypes, request `confidence` and `rationale`, store them; print a summary
  line on success.
- [ ] GitHub: symlinks/submodules → `skipped_entry` (non-gating) with a counter; consecutive
  failure breaker (stop after 3); `GH_TOKEN` fallback when `GITHUB_TOKEN` is empty.

### WP-G Source-of-truth schema fetchers: Glue, Postgres, Redshift (new)

Files: `etl_parser/schema/__init__.py`, `etl_parser/schema/base.py`, `etl_parser/schema/glue.py`,
`etl_parser/schema/postgres.py`, `etl_parser/schema/redshift.py`, `etl_parser/schema/catalog.py`,
`tests/test_schema_sources.py` (new, all transports mocked). Move `GlueSchemaProvider` out of
`workers/sql.py` into `schema/glue.py` and leave a re-export in `workers/sql.py`.

- [ ] `SchemaSource` protocol: `columns(dataset_id) -> list[str] | None` (existing contract)
  plus `catalog() -> dict` returning the agent `databases` list (`db_name`, `db_type`,
  `description`, `tables[] {table_name, description, schema[] {field_name, datatype,
  description, is_partition}, partition_key?, location?}`) and `relations() -> list[dict]`
  (`from_table, to_table, from_column, to_column, relation_type, source: "database"`).
- [ ] `GlueSchemaSource(session=None, *, profile=None, region=None, databases=None)`: boto3
  session honours `--aws-profile`/`AWS_PROFILE`/env keys/instance role; paginates
  `get_databases`/`get_tables`; partition keys flagged `is_partition`; table `Location`
  recorded and returned as an alias hint (`glue://db/t` ↔ `s3://...`) for identity merging;
  `db_type: "athena"`. Optional `.env` loading if `python-dotenv` is importable (never a
  hard dependency). Per-table caching; `columns()` served from the same cache.
- [ ] `PostgresSchemaSource(dsn=None, *, schemas=None)`: DSN from argument or
  `ETL_PARSER_POSTGRES_DSN`/`PG*` env vars only; uses `psycopg` (optional extra `postgres`);
  reads `information_schema.columns`, `pg_description` comments, PK/FK from
  `information_schema.table_constraints`/`key_column_usage`/`referential_constraints`;
  `db_type: "postgresql"`; dataset ids `postgres://<schema>/<table>`; relation_type
  `one_to_many` for FK, `one_to_one` when the FK column is also unique.
- [ ] `RedshiftSchemaSource(dsn=None, *, schemas=None, iam=False, profile=None, region=None,
  cluster_identifier=None, db_user=None)`: uses `redshift_connector` (optional extra
  `redshift`), IAM auth via profile when `iam=True`; reads `svv_columns`, `svv_table_info`,
  `pg_description`, `svv_constraints`/`pg_constraint` for PK/FK; `db_type: "redshift"`.
- [ ] `schema/catalog.py`: `write_schema_catalog(sources, out)` merges several sources into
  one `catalog.json` (`databases`, `relations`) in the exact shape of the user's
  `fetch_glue_schema.py` output, sorted for determinism; `DictSchemaProvider` continues to
  read that file.
- [ ] CLI (coordinate with WP-F: add the command group here, WP-F only documents it):
  `etl-parser schema fetch [--glue [--database NAME]...] [--postgres] [--redshift [--iam]]
  [--aws-profile P] [--region R] [--out catalog.json]`; `--postgres`/`--redshift` take no
  DSN argument on the command line, credentials come from the environment; the command
  refuses a DSN containing a password if given via `--dsn` (env only).
- [ ] `scan`/`run`: `--glue` uses `GlueSchemaSource` with `--aws-profile`; new
  `--schema-from-code` flag passed to the exporter.
- [ ] SDK support (user request 2026-09-19, mandatory): `etl_parser.schema` exports
  `GlueSchemaSource`, `PostgresSchemaSource`, `RedshiftSchemaSource`, `write_schema_catalog`;
  `etl_parser/__init__.py` re-exports them lazily (no driver import at package import).
  `ParserClient.fetch_schema(sources: list[SchemaSource]) -> dict` returns the catalog dict;
  `ParserClient.run`/`arun` and `etl_parser.scan` accept `schema=` (a `SchemaSource`, a
  catalog dict, or a path), `include_code_schema: bool = False`, and expose `schema_drift`
  on the result object and in `to_dict()`. Add tests in `tests/test_schema_sources.py` for
  the client surface with mocked sources. (Edit `etl_parser/sdk.py` and `etl_parser/pipeline.py`
  only for these additions; WP-F owns their other changes and merges after WP-G.)
- [ ] pyproject: extras `glue = ["boto3>=1.34"]`, `postgres = ["psycopg[binary]>=3.1"]`,
  `redshift = ["redshift_connector>=2.1"]`; nothing imported at module import time.
- [ ] Tests: mocked boto3 client, fake psycopg/redshift connections returning fixed rows;
  assert exact catalog shape, relation extraction, deterministic ordering, no credential in
  any serialized output, ImportError message when an extra is missing.

### WP-F CLI, pipeline, observability, packaging, docs (23, 24, 26, 35, 39, 40)

Files: `etl_parser/cli.py`, `etl_parser/pipeline.py`, `etl_parser/observability.py`,
`pyproject.toml`, `README.md`, `docs/cli.md`, `docs/sdk.md`, `docs/dependencies.md` (new),
`tests/test_cli_fixes.py` (new).

- [ ] Exit code gates only on `unsupported_syntax`; `analysis_note` and `skipped_entry` never
  gate. `run` exits 2 on provider authentication/authorization failure even without `--strict`.
- [ ] Missing lineage/prior/catalog files → `typer.BadParameter`; `export` creates `--out`
  parent directories.
- [ ] Library logging default: WARNING to stderr when used via `etl_parser.scan()`/
  `ParserClient`; CLI keeps INFO. Add help strings to every option lacking one.
- [ ] Remove `astroid` from dependencies; write `docs/dependencies.md` (what each package is
  for, what was dropped and why: grimp, libcst, jedi, sqllineage, sqlglotc, astroid).
- [ ] Scrub public-repo hygiene: replace `x-duke-*` examples with `x-example-*`; refer to the
  private SDK generically as "your organisation's Agent SDK (`agent-sdk`)"; keep the Nexus
  sentence generic.
- [ ] Docs: `--prior` semantics, `ai_confidence` fields, `relations_inferred`, exit codes,
  `description_source` values, `docs/sdk.md` line about invalid settings.

## Integration (coordinator)

- [ ] Merge WP-C, WP-D, WP-E, WP-G first (workers, AI, schema sources), run suite.
- [ ] Merge WP-B, regenerate golden, review diff.
- [ ] Merge WP-A, then WP-F.
- [ ] Full suite, ruff, byte-identical double scan of both fixtures, `uv build`.
- [ ] Re-run the live gateway checks (descriptions + fallback) and confirm `ai_confidence`
  appears on edges and catalog columns; secret scan.
- [ ] Update `docs/reports/2026-09-19-full-review.md` with a "Resolution" column.
