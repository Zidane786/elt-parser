# etl-parser

Static table and column lineage for SQL, PySpark, Pandas, Polars and Airflow.
Scans a repository, directory or file without importing or running its ETL code.
Local ZIP libraries are indexed and helper calls are followed within a bounded depth.

## Install

Requires Python 3.11 or newer.

```sh
uv sync
uv run etl-parser --help
# Or install from a local checkout:
pip install .
# Optional AWS Glue schema lookup and Bedrock description client:
pip install '.[glue]'
```

For an internal Nexus index, configure your normal pip/uv index settings. The package
does not store index credentials or connect to a service during an ordinary scan.

## Scan and explore

```sh
etl-parser scan ./etl-repo --schema catalog.json --out lineage.json
etl-parser scan ./jobs/orders.py --bindings bindings.json --out orders.json
etl-parser scan ./sql/orders.sql --engine postgres --out orders.json
etl-parser scan ./etl-repo --glue --region ap-south-1 --out lineage.json
etl-parser export catalog lineage.json --prior catalog.json --out catalog.updated.json
etl-parser export openlineage lineage.json --out events/
etl-parser impact lineage.json 'glue://warehouse/orders'
etl-parser impact lineage.json 'glue://warehouse/orders#amount' --upstream --depth 3
etl-parser products lineage.json
```

`python -m etl_parser` is equivalent to `etl-parser`. `--default-db` resolves unqualified
table names. `--dialect` overrides the SQL dialect inferred from `--engine` for SQL files.
Python SQL calls use the dialect of their executing API or statically known connection.
SQL and DataFrame aliases are traced back to source columns before edges are emitted.
Output names such as `amount_usd` remain the actual output column names.

The native JSON contains datasets, jobs, schedules, task-to-job links, table edges,
column edges, dependencies, provenance and unresolved items. Column IDs in impact queries
use `dataset_id#column`. Files are deterministic for the same source, schemas, bindings
and dependency versions. Scan exits with code 1 when `unsupported_syntax` is present;
other diagnostic kinds remain visible in JSON and the coverage summary.

### Dynamic tables and environment variables

```python
env = os.getenv("ENV", "dev")
table = f"warehouse.orders_{env}"
spark.table(table)
```

Supply explicit values in `bindings.json`:

```json
{"env:ENV": "prod"}
```

For an unknown Python variable named `env`, use `{"env": "prod"}`. Simple SQL-file
templates `${env}` and `{{ env }}` use the same bindings. Run separate scans for separate
environments; the parser cannot enumerate runtime deployment values.

Without a binding, a runtime name produces a diagnostic with `expression`, `partial_text`,
`symbols`, `assumptions`, `source_file`, `line`, and `remediation`. Environment defaults
are recorded as assumptions and are not promoted to concrete lineage. Missing symbols,
ambiguous columns and unsupported operations are reported rather than assigned invented
dataset names. Numeric padding, string concatenation, f-strings, `.format()` and common
percent formatting are folded when their inputs are known.

### ZIP helpers

Keep Python dependency ZIPs anywhere under the scanned directory. Module sources are
read directly from the archive; no extraction or imports occur. Provenance includes paths
such as `libs/helpers.zip!/company/transforms.py`. A single-file scan also indexes sibling
sources and archives under its containing directory. A direct ZIP scan treats its members
as entry files.

ZIP traversal paths, symlinks and ambiguous member names are rejected. Defaults limit each
source to 5 MB, each archive to 50 MB uncompressed and 2,000 members. Duplicate module
names require an unambiguous local match. Helper recursion/import depth is bounded.
Remote ZIP downloads, compiled extensions and nested ZIPs are not executed or unpacked.

## Coverage and confidence

| Area | Implemented analysis |
| --- | --- |
| SQL | CTAS, views, INSERT SELECT with positional target columns, CTEs, unions, MERGE branches/subqueries, UPDATE FROM, session temp tables, schema expansion and predicate dependencies |
| PySpark | Reads/writes, alias/select, computed and renamed columns, joins, grouping/aggregates, unions, filters, windows, SQL expressions and common scalar functions |
| Pandas | SQL/file reads, connection dialects, column projections/assignment, rename, joins/merge, common grouping/aggregation and writes |
| Polars | File/lazy reads, select, expressions, aliases, with_columns, join, group_by/agg, filters and file writes |
| Airflow | DAG contexts/constructors, task decorators, Python/Bash/SQL operators, shifts/lists/chain, task-only ordering bridges and locally resolvable cross-DAG references |
| Products | Both fixture product.yaml shapes, database ownership, schedule precedence, observed/declared dependency comparison and orchestration drift |

`exact` means the modeled expression resolves its column sources. `partial` means some
sources or semantics are missing. Schema-free pass-through frames may retain table lineage
while reporting `missing_schema` for unenumerated output columns. Unknown DataFrame
methods preserve known inputs and lower confidence. Dynamic control flow, arbitrary UDFs,
complex comprehensions, reflective imports, connection secrets and provider-specific
operators may need bindings or an extension. There is no promise of universal static
coverage. Review the diagnostics alongside the edges.

Airflow ordering and data flow are separate evidence in `job_dependencies[].sources`.
In-place writers are not assumed to precede other readers without declared task ordering.
Timedelta schedules retain their original text and do not claim an equivalent cron.
Postgres and Glue tables remain distinct identities even when their database/table names
match; physical aliasing needs explicit evidence.

OpenLineage exports are synthetic static snapshots, with stable run IDs and an epoch
timestamp when no timestamp is supplied. They do not assert that an ETL job actually ran.
Runtime OpenLineage ingestion remains outside this release, as deferred by the design.

## Python API and extensions

```python
from etl_parser import scan
from etl_parser.workers.sql import DictSchemaProvider

graph = scan("./etl-repo", schema=DictSchemaProvider("catalog.json"),
             bindings={"env:ENV": "prod"})
document = graph.to_document()
```

Every parser or orchestrator returns the same `WorkerResult`. Add a plugin without changing
the graph, impact analyzer or exporters:

```python
from etl_parser.pipeline import ParserRegistry, scan
from etl_parser.models import WorkerResult

class MyOrchestrator:
    name = "my_orchestrator"
    extensions = {".flow"}

    def accepts(self, source):
        return source.suffix == ".flow"

    def analyze(self, source, context):
        # Read source.text; use context.index to resolve scripts and ZIP modules.
        # Populate schedules, task_jobs and declared_upstream/downstream task IDs.
        return WorkerResult()

registry = ParserRegistry()
registry.register(MyOrchestrator())
graph = scan("./etl-repo", parsers=registry)
```

The CLI accepts `--plugin my_package:factory`; factories are explicitly trusted code
installed by the caller. Matching plugins compose, so use `ParserRegistry([...])` to
replace a built-in handler where necessary. Extend the declarative `SINKS` list in
`etl_parser/scanner/sinks.py` for additional I/O call signatures.

## Optional descriptions

```python
from etl_parser.describe.client import StubClient
from etl_parser.describe.engine import DescriptionEngine
from etl_parser.export.agent_catalog import export_agent_catalog

client = StubClient(['{"description": "Amount converted into cents"}'])
engine = DescriptionEngine(client)
catalog = engine.run(document, export_agent_catalog(document))
print(engine.warnings)
```

Provide any client implementing `complete(system, user) -> str`, or use the optional
`BedrockClient`. `etl-parser describe lineage.json --catalog catalog.json
--client my_package:client_factory --out enriched.json` explicitly invokes that client.
Lineage scanning never calls an LLM. Existing descriptions are preserved; exact identity
columns inherit descriptions; computed columns receive prompts containing their recorded
expressions and upstream descriptions. Invalid responses are skipped with warnings.

## Development

```sh
uv run pytest -q
uv run ruff check etl_parser tests
uv build
```

The test suite includes the 28-script ETL corpus, three product fixtures, regression tests
for alias/dynamic-name handling, local ZIP helpers, DAG dependencies, exports and the CLI.
See [implementation review](docs/implementation-review.md) for the review of the original
11-step plan and explicit deviations.
