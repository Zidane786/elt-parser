# etl-parser

Static table and column lineage for SQL, PySpark, Pandas, Polars and Airflow.
Scans a repository, directory or file without importing or running its ETL code.
Local ZIP libraries are indexed and helper calls are followed within a bounded depth.

Usage guides: [CLI commands and configuration](docs/cli.md) ·
[Python SDK and backend integration](docs/sdk.md).

Quick navigation: [commands and help](#complete-command-reference),
[catalog template](#catalog-input-template), [runner selection](#runner-selection-and-gateway-settings),
[supported analysis](#coverage-and-confidence), [AI modes](#optional-ai-lineage-and-shared-descriptions),
[GitHub](#github-repositories-without-cloning), [logs](#logs-and-metrics),
[extensions](#python-api-and-extensions).

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
ZIP blobs inside a GitHub repository can also be inspected through the GitHub source
provider. Arbitrary external ZIP downloads, compiled extensions and nested ZIPs are
not executed or unpacked.

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

### Python SDK for backend services

See the [complete SDK guide](docs/sdk.md) for synchronous and asynchronous clients,
all configuration fields, service integration, exports, logging and source providers.
The [CLI guide](docs/cli.md) covers shell usage and its differences from the SDK.

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
The legacy `describe` command only enriches descriptions, never mappings. The separate
`run --ai-lineage fallback|improve` modes can propose audited lineage additions.

All description calls use your **gdtc-agent-sdk**, tested against distribution
`agent-sdk==1.3.1`, and its `BedrockInvokeLambdaRunner` or `AnthropicRunner`. The old direct Bedrock client
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

## Logs and metrics

Console logging is on by default (JSON events on **stderr**, leaving stdout JSON clean).
All CLI commands accept `--log-dir` and `--log-level`. Detailed file logs include DEBUG
findings even when the console level is INFO or WARNING.

```sh
etl-parser scan ./etl-repo --schema catalog.json --out lineage.json \
  --log-dir ./run-logs --log-level INFO
etl-parser describe lineage.json --catalog catalog.json --out enriched.json \
  --lambda-arn YOUR_FUNCTION --model YOUR_MODEL --aws-profile YOUR_PROFILE \
  --log-dir ./run-logs
```

Each run gets a unique UTC/run-ID directory containing `events.jsonl`, `metrics.json`,
and `manifest.json`. Events identify the actor, stage, file/job, reason, run/span IDs
and AI request IDs. They cover source discovery/imports/ZIPs, parser findings,
diagnostics, graph building, exports, description eligibility, model invocation and
response acceptance/rejection. The manifest includes redacted configuration.

Metrics include file/byte counts, raw findings and final graph counts, confidence
buckets, diagnostic categories, per-stage timing, whole-run throughput, bounded
recent latency percentiles, AI attempts/completions/failures, SDK-reported token/cache
usage, and existing/inherited/generated/skipped/failed description counts. Cost and
independently measured accuracy remain `null` when unavailable—not fabricated values.
Zero SDK usage can mean missing provider reporting; it is counted separately.

`scan`, `run` and `describe` accept `--log-max-bytes` (default 10 MB per event file) and
`--log-max-files` (default 20 per run). A single event may exceed the rotation threshold;
fields are bounded and truncation is marked. Reaching the file limit or encountering
a disk error visibly marks persisted logging incomplete; deterministic work can
continue, but subsequent description calls are skipped if their persistent audit
has failed. Logs are never silently deleted to make space. Retention across runs is
caller-managed. No raw source, prompts, responses, provider exception messages or
SDK raw usage payloads are logged. Metadata redaction is best-effort: treat logs as
sensitive because dataset names, file paths and other structural identifiers remain.

Python callers can use `scan(path, log_dir=..., log_level="INFO")` and
`engine.run(doc, catalog, log_dir=...)` / `await engine.arun(...)`. Advanced callers
can supply a `RunObserver` to share counters and customize rotation limits, then
call `observer.finish()` when their run ends. Existing application logging handlers
are not replaced. No telemetry is uploaded.

## Optional AI lineage and shared descriptions

`run` always parses deterministically first. Both AI lineage and descriptions default
off. AI modes are additive, evidence-checked assistance—not proof of correctness.

| Controls | Behavior |
| --- | --- |
| Defaults | Deterministic lineage only; zero AI calls |
| `--descriptions` | Generate missing supported descriptions; reuse the same call for a separate background lineage comparison where feasible |
| `--descriptions --no-background-comparison` | Descriptions without asking for shadow lineage |
| `--ai-lineage fallback` | Review only files with diagnostics, partial mappings or input/output jobs missing column edges |
| `--ai-lineage improve` | Review selected ETL files, including confidently parsed files |
| `--ai-lineage fallback --descriptions` | Share lineage and description work in one response per selected file |
| `--dry-run` | Parse and explain proposed AI work without constructing/calling the SDK |

```sh
# Defaults: no credentials or AI required.
etl-parser run ./etl --schema catalog.json --out-dir ./artifacts --log-dir ./logs

# File-level fallback, with descriptions in the same response where requested.
etl-parser run ./etl --ai-lineage fallback --descriptions \
  --lambda-arn YOUR_FUNCTION --model YOUR_MODEL --aws-profile YOUR_PROFILE \
  --max-calls 20 --out-dir ./artifacts --log-dir ./logs

# Descriptions and opportunistic background comparison; main lineage stays unchanged.
etl-parser run ./etl --descriptions --lambda-arn YOUR_FUNCTION --model YOUR_MODEL

# Broader review, restricted to selected files. Repeating globs adds alternatives.
etl-parser run ./etl --ai-lineage improve --include 'jobs/*.py' --exclude '*secret*' \
  --lambda-arn YOUR_FUNCTION --model YOUR_MODEL --dry-run
```

With AI lineage off, **no extra call is ever scheduled just to compare lineage**.
If descriptions already exist or can be inherited, no background call is made.
If source context is too large, comparison is dropped and description-only work
is attempted within the same budget. Explicit lineage modes may make their own
authorized calls. There is no implicit paid retry, repair call, or source chunking.

Policy code chooses files and accepts changes; the model has no tools and cannot
expand its scope. Responses must match a strict versioned schema. Evidence checks
cover file/job ownership, source digest, lines, exact quotes, physical identifiers
and known schema columns. This does not prove semantic correctness. Exact baseline
mappings and all original diagnostics remain; conflicting changes are deferred.
Accepted AI additions are marked `inferred`, with model/request/evidence provenance.
If a file's audit fails during processing, its AI mutations are rolled back.

Descriptions preserve prior text (`--prior catalog.json`) and inherit exact identity
descriptions without a model. Source-backed `run` batches remaining requested targets
by file and asks for structured descriptions plus optional lineage. Descriptions
contradicting the effective mappings are not published; partial mappings need accepted
AI support. Legacy `describe lineage.json` remains evidence-only, per-column enrichment:
it cannot compare missing source snapshots and never schedules a source audit.

Controls and default bounds:

- `--max-calls 20`, `--max-output-tokens 16000`, `--max-context-chars 60000`.
- `--max-total-tokens` optionally bounds conservative reservations and reported usage.
  Missing usage retains the reservation; this is not a billing guarantee.
- `--timeout-seconds 300`, `--deadline-seconds 3600` for the AI stage; calls are serial.
  Cancelling a timed-out request cannot guarantee an already-running Lambda stops billing.
- `--include` / `--exclude` select AI files, not deterministic source inventory.
- `--strict` exits nonzero for unresolved or incomplete work; without it partial artifacts
  remain usable. Configuration/scan failures still fail normally.
- `--config settings.json` accepts `AnalysisConfig` fields; explicit CLI flags override
  environment-backed provider settings, then config, then defaults. Unknown fields fail.
- Existing schema/bindings/products/engine/dialect/Glue/trusted-plugin controls also work.

The 16,000-token value is a maximum, not a request to fill the entire response. A larger
limit can increase latency and token-budget reservations, and must be supported by the
selected gateway/model. Override it with `run --max-output-tokens 8000`,
`AnalysisConfig(max_output_tokens=8000)`, or `describe --max-tokens 8000`.
The longer timeout/deadline reduce premature expiry; they cannot guarantee provider
availability or complete every file. Increase `--max-calls` explicitly for larger runs.

Failure decisions distinguish `call_limit`, `deadline_exceeded`, `output_token_limit`,
`response_schema_invalid`, timeouts and provider HTTP errors. HTTP status and a numeric
`retry_after_seconds` (when supplied) are logged without provider response bodies.
On HTTP 401/403/404/429, remaining eligible AI files are skipped as
`provider_unavailable`; deterministic results remain available. The framework does
not sleep/retry automatically. Correct credentials/endpoint or wait for the provider's
rate/quota window, then rerun the affected files with `--include`. There is no persisted
automatic resume or global circuit breaker across independent service requests.

Each `--out-dir` gets an exclusive `run_<id>/` folder with `lineage.json` (effective),
`lineage.deterministic.json`, `catalog.json`, `decisions.json`, `changes.json`,
`work-plan.json`, and a final atomic `manifest.json` with config/source/output digests.
When reviewed, `lineage.ai.json` and `lineage.comparison.json` are separate artifacts.
Comparisons report column/table additions, absences, agreements and textual expression
differences; they are parser-assisted and **not independent accuracy measurements**.
Raw code is absent from event logs, but lineage/change artifacts contain expressions;
protect those artifacts as source-sensitive data. File permissions are owner-private.

```python
from etl_parser.ai_analysis import AnalysisConfig, analyze, analyze_async
from etl_parser.artifacts import write_analysis

result = analyze("./etl", config=AnalysisConfig(), log_dir="./logs")
folder = write_analysis(result, "./artifacts")
# In async applications: result = await analyze_async("./etl", config=...)
```

Use your trusted private Agent SDK installation described above. Enabling AI sends
selected file content and lineage evidence to your configured Lambda/model service or gateway.
`--aws-profile` selects credentials; it does not perform AWS SSO/login for you.

## Runner selection and gateway settings

Both `run` and `describe` accept the same runner options. There is no provider fallback:
only the selected runner is constructed. AI-off and dry-run do not construct either runner.

| `--runner` value | SDK implementation | Required for an actual AI call |
| --- | --- | --- |
| `lambda-bedrock-invoke` (default) | `BedrockInvokeLambdaRunner` | `--lambda-arn`, `--model`; normal AWS credentials or `--aws-profile` |
| `lbi` | Alias for `lambda-bedrock-invoke` | Same options; manifests normalize to the long name |
| `anthropic` | `AnthropicRunner` | `--base-url`, API key and `--model`; no Lambda ARN or AWS credentials |

`bedrock` is deliberately **not** a runner option: the SDK's direct Bedrock runner is
different from Lambda Bedrock Invoke and is not wired into this release.

```sh
etl-parser run ./etl --descriptions --runner lbi \
  --lambda-arn YOUR_FUNCTION --model YOUR_BEDROCK_MODEL --aws-profile YOUR_PROFILE

# Set ANTHROPIC_API_KEY securely in your environment first; do not commit it.
# Base URL includes the gateway prefix, not /v1/messages (the SDK adds that).
etl-parser run ./etl --descriptions --runner anthropic \
  --base-url https://gateway.example/api --model YOUR_MODEL \
  --log-dir ./logs --out-dir ./artifacts

etl-parser describe lineage.json --catalog catalog.json --out enriched.json \
  --runner anthropic --base-url https://gateway.example/api --model YOUR_MODEL

# Alternatively, supply your compatible gateway through an environment variable.
# Set ANTHROPIC_API_KEY securely; never put the key in committed settings.
export ANTHROPIC_BASE_URL=https://gateway.example/api
etl-parser run ./etl --runner anthropic --model YOUR_MODEL \
  --base-url "$ANTHROPIC_BASE_URL" \
  --ai-lineage improve --descriptions --max-output-tokens 16000 \
  --log-dir ./logs --out-dir ./artifacts

# Optional custom headers, only if your gateway requires them:
etl-parser run ./etl --descriptions --runner anthropic \
  --base-url https://gateway.example/aigw --model YOUR_MODEL \
  --extra-headers '{"x-duke-mode":"invoke","x-duke-stream":"true"}'

# Alternatively, headers.json contains that same JSON object:
etl-parser run ./etl --descriptions --runner anthropic \
  --base-url https://gateway.example/aigw --model YOUR_MODEL --extra-headers-file headers.json
```

No extra headers are added by default. The gateways tested for this project use **no
extra headers**. Header values must be strings; JSON booleans such as `true` must be
written as `"true"`. `--extra-headers` and `--extra-headers-file` are mutually exclusive.
Header overrides are passed to the SDK, including custom auth/routing headers; transport
headers `Host`, `Content-Length`, `Transfer-Encoding`, newline injection, and duplicate
case-insensitive names are rejected. A custom `x-duke-stream` header is forwarded as
gateway metadata; it does **not** switch the parser to `complete_stream`. The analysis
pipeline expects a complete Anthropic-compatible JSON response, not an SSE stream.

Use HTTPS roots without embedded credentials/query strings. Redirects are not followed.
API keys are sent as the SDK's `x-api-key`; custom auth overrides are an explicit caller
choice. Keys/header values are excluded from serialized configs/manifests and ordinary
logs. `--api-key` exists, but environment variables are safer than shell history/process
arguments. Never place real credentials in committed JSON config/header files. Explicitly
provided SDK runners in Python remain caller-owned; factory-created HTTP runners are closed.

| Environment variable | Equivalent option / behavior |
| --- | --- |
| `ETL_PARSER_RUNNER` | `--runner` |
| `ETL_PARSER_MODEL` | `--model` (no hard-coded model) |
| `ETL_PARSER_LAMBDA_ARN` | `--lambda-arn` |
| `AWS_REGION` | `--region` |
| `AWS_PROFILE` | `--aws-profile` |
| `ANTHROPIC_API_KEY` | `--api-key` |
| `ANTHROPIC_API_BASE_URL` | `--base-url` (first choice) |
| `ANTHROPIC_BASE_URL` | `--base-url` (fallback environment name) |
| `GITHUB_TOKEN` / `GH_TOKEN` | GitHub read token, separate from AI authentication |

For `run`, precedence is explicit CLI > provider environment > JSON config > defaults.
Header options replace the whole configured header object; `--extra-headers '{}'` clears it.
Example non-secret `settings.json`:

```json
{
  "runner": "anthropic",
  "base_url": "https://gateway.example/aigw",
  "model": "YOUR_MODEL",
  "ai_lineage": "fallback",
  "descriptions": true,
  "background_comparison": true,
  "extra_headers": {},
  "max_calls": 10,
  "max_output_tokens": 16000,
  "max_context_chars": 60000,
  "max_total_tokens": 100000,
  "timeout_seconds": 300,
  "deadline_seconds": 3600,
  "include": ["*.py", "*.sql"],
  "exclude": ["*secret*"]
}
```

Run it with `etl-parser run ./etl --config settings.json`. Source paths, schema, products,
output/log paths and strictness are CLI/API scan settings, not `AnalysisConfig` fields.

## Catalog input template

[catalog.template.json](catalog.template.json) is a valid, non-secret example of the
agent catalog accepted by this framework. Replace its example names/columns with your
actual physical schema; it is not your production catalog and is not loaded implicitly.

```sh
etl-parser scan ./etl --schema catalog.template.json --out lineage.json
etl-parser run ./etl --schema catalog.template.json --prior catalog.template.json \
  --out-dir ./artifacts --log-dir ./logs
etl-parser export catalog lineage.json --prior catalog.template.json --out catalog.updated.json
```

`--schema` supplies known column names for resolution/star expansion/AI validation.
`--prior` supplies existing descriptions and metadata to preserve in the output catalog.
Use both if you need both; `--schema` alone does not seed prior descriptions.
The legacy `describe --catalog` takes the catalog to enrich. The native `lineage.json`
is a different format; do not pass it as a schema catalog.

| Catalog field | Meaning |
| --- | --- |
| `databases[].db_name` | Physical database/schema namespace, e.g. `raw` |
| `databases[].db_type` | Engine/catalog type, e.g. `glue`, `postgres`, `mysql`, `sqlite` |
| `tables[].table_name` | Table name within that database, not a SQL alias |
| `tables[].dataset_id` | Canonical identity such as `glue://raw/orders`; exporter fills it from observed datasets |
| `tables[].schema[].field_name` | Actual column name |
| `datatype` | Optional preserved metadata; current schema lookup primarily uses column names |
| `description` | Existing descriptions are preserved; empty strings are eligible for enrichment |
| `verify`, `is_partition`, other flags | Preserved caller metadata, not instructions to execute/verify/mask actual data |
| `relations` | Preserved application metadata; does not itself establish static lineage |
| `scripts` | Rebuilt from observed jobs with reads/writes/dependencies/schedules; may start empty |
| `schedules` | Rebuilt from parsed orchestration metadata; may start empty |
| `lineage.column_edges`, `lineage.unresolved` | Exported evidence/diagnostics; may start empty in an input template |

The small schema-only alternative is `{"raw":{"orders":["order_id","amount","order_date"]}}`.
This shorthand is accepted by `--schema`, not a replacement for the description catalog.
Schema lookup currently indexes `database.table` case-insensitively; use a catalog
appropriate to the scanned engine when identical names have different physical schemas.

## GitHub repositories without cloning

```sh
etl-parser scan https://github.com/OWNER/REPO --ref COMMIT_OR_BRANCH --path jobs \
  --out lineage.json --log-dir ./logs
etl-parser run https://github.com/OWNER/REPO --ref COMMIT_SHA --path jobs/example.py \
  --out-dir ./artifacts --log-dir ./logs
```

Use a repository URL, with branch/tag/commit and file/directory/ZIP path as separate
options. The default branch is resolved once; all subsequent tree/blob reads use
that snapshot. No clone, checkout, source-file materialization or code execution occurs.
The same source index supports Python helpers, in-repository ZIPs, SQL, DAGs and
`product.yaml`. Unsupported files are counted/skipped rather than parsed as code.

Private repositories use `GITHUB_TOKEN` or `GH_TOKEN`; never put tokens in the URL.
API requests use the fixed GitHub API host and reject redirects. Truncated recursive
trees fall back to subtree walking. Symlinks, submodules, LFS pointers, binary/oversized
sources and read failures are reported; external objects are not fetched implicitly.

Defaults: 20,000 inventory entries, 5 MB per source/blob, 100 MB cumulative downloaded
and indexed archive source bytes, 30-second request timeout and two retries for
retryable failures. Rate-limit waits over 30 seconds stop visibly instead of silently
waiting for hours. Advanced Python callers can configure `GitHubSource` limits and
pass it as `scan(..., source_provider=provider)` or through `analyze`. Requests are
serial; GitHub Enterprise/App auth and cross-run disk caches are not implemented.
Filesystem parity means equal semantics for the same supported snapshot—not equal
network latency or access to unbounded/external dependencies.

## Complete command reference

See the [complete CLI guide](docs/cli.md) for every command, option, configuration
setting, environment variable, output artifact and exit behavior.

## Development and explicit live checks

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
The [follow-up delivery plan](docs/superpowers/plans/2026-09-17-ai-lineage-observability-github.md)
records implemented AI modes, same-call background comparisons, logs/metrics and
clone-free GitHub scanning. See the [plain-language delivery report](docs/reports/2026-09-18-delivery-report.md)
for the request-to-feature mapping, test evidence and explicit limits.
SDK-dependent tests are explicitly skipped when the private SDK is absent; the base suite
still tests the missing-dependency message and the no-SDK/no-AWS scanning boundary. The
integration tests use the real SDK with mocked Lambda transport, not paid model calls.

For a repeatable local performance probe (not an accuracy/production-capacity claim):

```sh
python -m benchmarks.scan_benchmark --synthetic-files 500 --repeats 3
python -m benchmarks.scan_benchmark --source ./etl --schema catalog.json
```

An explicitly paid gateway check is also available. It reads `ANTHROPIC_API_KEY`
or prompts with hidden terminal input; it never writes credentials. Without `--source`
it uses synthetic ETL. Supplying `--source` authorizes sending selected source context
to the configured gateway. Results, audit artifacts and metrics are kept in a temporary
private folder unless `--out-dir` is provided. It never adds custom headers.

```sh
python -m benchmarks.gateway_check --help
python -m benchmarks.gateway_check --execute \
  --base-url https://gateway.example/api --model YOUR_MODEL --max-calls 1
python -m benchmarks.gateway_check --execute \
  --base-url https://gateway.example/aigw --model YOUR_MODEL \
  --source ./etl --schema catalog.json --ai-lineage fallback --max-calls 4
```

Manual check options: required `--execute`, `--base-url`, `--model`; optional
`--source`, `--schema`, `--out-dir`, `--ai-lineage off|fallback|improve` (improve),
`--descriptions/--no-descriptions` (on),
`--background-comparison/--no-background-comparison` (on), `--max-calls` (1),
`--max-output-tokens` (16000), `--timeout-seconds` (300), `--deadline-seconds` (3600),
and repeatable `--include` AI file globs. The checker uses the public Python client.
This explicit diagnostic tool's descriptions-on default is different from the main
`etl-parser run` defaults. Live checks are never part of routine tests/CI.

The performance probe accepts `--source`, `--schema`, `--synthetic-files` (200),
`--repeats` (3), `--output`, and `--help`; it makes no AI/network requests.
