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
# Optional AWS Glue schema lookup:
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

For Airflow `SQLExecuteQueryOperator(conn_id="warehouse", ...)`, explicitly bind the
connection dialect using `{"connection:warehouse": "postgres"}`. Credentials are not
needed: this tells the parser which SQL grammar and dataset namespace to use.

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

Mappings are deterministic: SQLGlot and Python AST/DataFrame tracking resolve lineage.
The LLM only proposes descriptions from that evidence; it never creates or repairs mappings.

All description calls use your **gdtc-agent-sdk**, tested against distribution
`agent-sdk==1.3.1`, and its `BedrockInvokeLambdaRunner`. The old direct Bedrock client
and custom client protocol have been removed. No model or Lambda ARN is hard-coded.
Install the SDK from your trusted internal distribution or local checkout, **not an
unverified public package with the same name**:

```sh
uv pip install --python .venv/bin/python /path/to/gdtc-agent-sdk
.venv/bin/etl-parser describe lineage.json --catalog catalog.json --out enriched.json \
  --lambda-arn YOUR_FUNCTION_NAME_OR_ARN --model YOUR_BEDROCK_MODEL_ID \
  --region ap-south-1 --aws-profile YOUR_PROFILE
```

`ETL_PARSER_LAMBDA_ARN`, `ETL_PARSER_MODEL`, `AWS_REGION`, and `AWS_PROFILE` are also
supported. Credentials use the SDK's normal AWS credential chain; omit `--aws-profile`
for an IAM role. The default envelope targets the SDK's Lambda Web Adapter `/bedrock`
route. Use `--no-web-adapter` for a plain Lambda handler accepting `{modelId, payload}`.
This command sends lineage evidence to your configured model service and may incur charges.

```python
from etl_parser.describe.client import bedrock_lambda_runner
from etl_parser.describe.engine import DescriptionEngine
from etl_parser.export.agent_catalog import export_agent_catalog

runner = bedrock_lambda_runner("YOUR_FUNCTION_NAME_OR_ARN", region="ap-south-1")
engine = DescriptionEngine(runner, model="YOUR_BEDROCK_MODEL_ID")
catalog = engine.run(document, export_agent_catalog(document))
# Inside an async application: catalog = await engine.arun(document, catalog)
print(engine.warnings)
```

For offline tests, inject `agent_sdk.testing.FakeLLMRunner` into the same engine.
The SDK is installed separately because it is a private dependency; base scans and CI do
not require access to your private registry. Lineage scanning never imports the SDK or
calls an LLM. Existing descriptions are preserved; exact identity
columns inherit descriptions; computed columns receive prompts containing their recorded
expressions and upstream descriptions. Invalid, empty, truncated, blocked, or tool-request
responses are skipped with warnings. Partial lineage is never sent for enrichment.

## Development

```sh
uv run pytest -q
uv run ruff check etl_parser tests
uv build
# After installing the private SDK, also run its integration contracts:
.venv/bin/python -m pytest tests/test_describe.py -q
```

The test suite includes the 28-script ETL corpus, three product fixtures, regression tests
for alias/dynamic-name handling, local ZIP helpers, DAG dependencies, exports and the CLI.
See [implementation review](docs/implementation-review.md) for the review of the original
11-step plan and explicit deviations.
SDK-dependent tests are explicitly skipped when the private SDK is absent; the base suite
still tests the missing-dependency message and the no-SDK/no-AWS scanning boundary. The
integration tests use the real SDK with mocked Lambda transport, not paid model calls.
