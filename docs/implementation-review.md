# Implementation review

Reviewed against `superpowers/plans/2026-09-15-etl-parser-core.md` and its design spec.
This review supersedes the original plan's unchecked implementation checklist.

## The 11 steps

| Step | Delivered | Evidence / boundary |
| --- | --- | --- |
| 1. Package, models, identity | Installable package, canonical datasets, structured diagnostics and provenance | Model/identity tests; wheel and source distribution build |
| 2. String folding and sinks | f-strings, constants, formatting, explicit bindings, auditable environment defaults, declarative I/O signatures | String, sink and dynamic-reference regressions |
| 3. SQL worker | CTE/alias tracing, INSERT target positions, MERGE branches/subqueries, UPDATE FROM, temp tables, schema lookup, per-statement recovery | SQL fixture and regression tests; missing/ambiguous columns remain partial |
| 4. Scanner and Python worker | Repository/path indexing, shared Python frame tracker, bounded local/helper imports, ZIP modules | All 28 fixture jobs' reads/writes checked against reviewed catalog corrections; ZIP tests |
| 5. Airflow | Context/constructor/decorator DAGs, task jobs, Bash/SQL/Python operators, declared dependencies, task-only bridges, local cross-DAG references | Multi-DAG, TaskFlow, list/shift, sensor and product fixture tests |
| 6. Registry and graph | Both product YAML shapes, database ownership, schedule precedence, data/DAG dependency evidence, in-place-writer rule | Product fixture and graph integration tests |
| 7. Impact and drift | Upstream/downstream hops, column traversal, products, cross-product edges, declared/observed drift | Meter-to-regulatory fixture traversal and CLI tests |
| 8. Exports and CLI | Native JSON, agent catalog, OpenLineage models, scan/export/impact/products commands | Deterministic round-trip, golden scripts, preservation and CLI tests |
| 9. DataFrame column lineage | Shared PySpark/Pandas/Polars frame analysis, joins, aliases, calculations, grouping, unions, filters and windows | Physical-column projection/expression/window/union tests; unsupported methods emit diagnostics |
| 10. Descriptions | Caller-supplied client, stub and optional Bedrock client, grounded prompts, identity inheritance, dependency order, malformed-response warnings | Description tests with no network calls |
| 11. Documentation and end-to-end | README, frozen corrected scripts output, fixture/integration tests, packaging and CI workflow | `pytest`, `ruff`, and `uv build` |

## Corrections to the plan and fixtures

- The original SQL worker dropped CTEs attached to INSERT statements and could leak temp
  table mappings across files. These are covered by regressions now.
- The original CTE test expected `customers.id` for a projection of `customers.customer_id`.
  The assertion now checks the actual projected column. SQL locations use the enclosing
  statement span: the energy-sold fixture starts at line 13, not line 15.
- Five catalog read lists were incomplete: `dim_customer_scd2` reads its existing target,
  both ingest jobs read S3, `mart_customer_ltv` reads the customer dimension, and
  `mart_funnel_conversion` reads page views. Tests explicitly verify those corrections.
- Thirteen schedule strings in the original catalog differ from code headers. Source
  headers are authoritative. The original fixture is preserved; the corrected scripts
  contract is frozen in `tests/fixtures/catalog_scripts_golden.json`.
- Environment defaults are assumptions, not verified runtime values. An explicit binding
  is required before those names enter concrete lineage. This intentionally strengthens
  the original default-folding behavior.
- Timedelta intervals stay as text with `cron=None`; equating a start-anchored interval to
  wall-clock cron would change its meaning. Invalid cron values are rejected.
- New parser and orchestrator names use open strings in provenance/schedules. Additional
  engines and orchestration plugins can share the model without editing Literal enums.
- ZIP dependencies were not in the original plan. Sources are indexed directly from ZIP
  members with limits and explicit archive provenance; archive code is never executed.

## Explicit limits

The package is a static analyzer with bounded coverage. It cannot prove all behavior of
arbitrary Python, SQL dialects or orchestration libraries. Runtime-generated names,
unmodeled DataFrame operations, complex comprehensions, reflective imports, arbitrary
UDFs and unresolved external jobs remain diagnostics or partial edges. SQL templates
support simple named substitutions; this is not a Jinja/dbt execution environment.

Generic SQL operators without a known dialect, provider-specific Glue/EMR job mappings,
and external DAGs absent from the repository require explicit extensions or metadata.
Unsupported call/control-flow diagnostics must be reviewed before using a graph as a
complete inventory. Missing schemas can prevent complete output-column enumeration.

Logical schema names in the product fixtures sometimes appear in both Postgres and Glue.
Those physical datasets are not merged based only on matching spelling. Native dataset
identity remains engine-aware; cross-system aliases need explicit evidence.

The live Glue and Bedrock adapters are tested through local stubs/model contracts, not
against a production AWS account. OpenLineage exports are synthetic static events, not
execution records. Runtime OpenLineage ingestion was explicitly deferred by the design.

## Extension architecture

`RepoScanner` indexes sources once. `ParserRegistry` selects handlers through
`accepts(source)`; handlers receive `ScanContext` with index, schema, bindings and SQL
defaults. Parsers and orchestrators emit `WorkerResult`, so graph building, dependency
resolution and exports do not depend on individual parser implementations. Plugin
failures become source-located diagnostics while other files continue.

The Python frame interpreter and SQL worker stay separate from the graph. A new parser
may replace a built-in handler, while a new orchestrator may add schedules and task-job
links using the same contract. Explicit CLI plugin factories are trusted user-installed
code; scanned repository modules are always data.
