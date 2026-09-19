# Python SDK usage guide

Embed the installed `etl-parser` package directly in a script, worker or async backend.
No subprocess or HTTP server is required. The [CLI guide](cli.md) documents equivalent
commands. All gateway URLs, models and function names here are placeholders.

## Install and public entry points

Requires Python 3.11+. Run `pip install .` from the checkout, or install your approved
internal distribution. Install `.[glue]` for optional AWS Glue schema lookup.
AI additionally requires your organisation's Agent SDK (`agent-sdk`) from its trusted
internal distribution or source checkout, not an unrelated public package with a
similar name.

```python
from etl_parser import (
    AnalysisConfig, AnalysisRun, ParserClient, analyze, analyze_async, scan,
)
```

| API | Purpose | Return value |
| --- | --- | --- |
| `ParserClient.run(source, **options)` | Sync analysis with isolated logs/metrics and optional artifacts | `AnalysisRun` |
| `await ParserClient.arun(source, **options)` | Async backend equivalent | `AnalysisRun` |
| `scan(source, **scan_options)` | Deterministic scan, no AI | `LineageGraph` |
| `analyze(source, **options)` | Lower-level sync analysis | `AnalysisRun` |
| `await analyze_async(source, **options)` | Lower-level async analysis/custom observation | `AnalysisRun` |

`ParserClient` adds run metadata, metrics and optional artifact/log paths to the result.
The lower-level analysis functions do not populate those client-owned metadata fields.
Sync analysis entry points raise inside a running event loop; use the async method.
Deterministic scanning and help require no AI SDK or provider credentials.

## Synchronous use

```python
from etl_parser import AnalysisConfig, ParserClient
from etl_parser.workers.sql import DictSchemaProvider

client = ParserClient(
    config=AnalysisConfig(),  # AI lineage and descriptions both OFF
    log_dir="./logs",         # omit for console-only logging
    log_level="INFO",
)
result = client.run(
    "./etl",
    schema=DictSchemaProvider("catalog.json"),
    bindings={"env:ENV": "prod"},
    out_dir="./artifacts",    # omit for in-memory results only
)
print(result.status, result.run_id)
response_payload = result.to_dict()  # JSON-compatible, excludes raw source snapshots
```

Local directories, individual SQL/Python files and local ZIP entry archives are supported.
Repository ZIP libraries are indexed as helpers without importing/executing them.
Use actual physical schema names in [catalog.template.json](../catalog.template.json).
`schema` supplies column knowledge; `prior` supplies catalog metadata/descriptions.

```python
import json
from pathlib import Path

prior = json.loads(Path("catalog.json").read_text())
result = client.run("./etl", prior=prior, schema=DictSchemaProvider(prior))
```

Unlike CLI options, SDK schema/binding/prior inputs are Python objects:
`schema` is a provider, `bindings` is a dictionary, and `prior` is a catalog dictionary.
`products` remains a registry-file path. The client does not implicitly load a catalog.

## Async service integration

```python
import asyncio
from etl_parser import ParserClient

client = ParserClient(log_level="WARNING")

async def process_jobs(authorized_paths):
    limit = asyncio.Semaphore(4)  # choose a bound appropriate for your backend/provider

    async def process(path):
        async with limit:
            result = await client.arun(path)
            return result.to_dict()

    return await asyncio.gather(*(process(path) for path in authorized_paths))
```

Use a shared service/queue concurrency bound across requests, not a new semaphore for
each HTTP request. Each invocation has separate results, provider clients, logs and metrics.
Scanning/GitHub reads and artifact writing run in worker threads, not on the event loop.
Caller-supplied runners, source providers, schema providers and custom parsers must be
safe for your concurrency pattern; create separate instances where necessary.

No bundled HTTP server, authentication layer, durable queue or cross-request rate limiter
is provided. Authorize source paths/repositories and keep runner configuration/credentials
server-controlled. Do not let untrusted request bodies choose arbitrary local paths,
plugin modules, credential profiles or provider URLs.

AI file failures return partial results. Scan/export failures raise. Provider
**misconfiguration** is checked once before the file loop and raises
`AnalysisPolicyError("provider_configuration_invalid")` rather than being reported per
file: a missing Lambda ARN, an absent Agent SDK or an unusable base URL is a settings
error, not an outage. Per-file provider failures during the loop are still recorded as
decisions with a `reason` (`provider_authentication_failed`, `provider_rate_limited`,
`timeout`, `response_schema_invalid`, `internal_error` for a non-provider fault).
Cancellation propagates and finalizes the client's log summaries. It does not forcibly
stop a scan/export already running in a thread, or guarantee that remote billing stops.

## ParserClient configuration and call options

Constructor: `ParserClient(config=None, log_dir=None, log_level="INFO")`.
Configuration is validated and copied on construction and per invocation.

Both `run` and `arun` accept:

| Argument | Meaning |
| --- | --- |
| `source` | Required local path or `https://github.com/OWNER/REPO` |
| `config` | Optional `AnalysisConfig` or dictionary; **replaces**, does not merge with, client configuration |
| `runner` | Optional injected private-SDK-compatible runner; caller-owned and never closed by the client |
| `prior` | Optional existing catalog dictionary |
| `out_dir` | Optional artifact parent directory; unique `run_<id>` child |
| `log_dir`, `log_level` | Per-call overrides of constructor logging settings |
| `log_max_bytes` | Per-file log bound; default 10,000,000, minimum 1,024 |
| `log_max_files` | Maximum log files; default 20, minimum 1 |
| scan options | All entries in the next table |

The client owns its observer and rejects `observer=`. Use `analyze_async` for advanced
caller-owned observation. Factory-created provider clients are closed automatically.
Injected runners must support your event-loop/concurrency usage and be closed by you.
No provider fallback occurs. Provider requirements are checked when a call is needed;
AI-off, dry-run, filtered or already-described work need not initialize a provider.

### Scan options

These options work with `scan`, `ParserClient.run/arun`, and lower-level analysis:

| Python argument | Default / meaning | CLI equivalent |
| --- | --- | --- |
| `schema` | `None`; `SchemaProvider.columns(dataset_id)` returns names or `None` | `--schema` / `--glue` |
| `bindings` | `None`; string-to-string dictionary | `--bindings` file |
| `default_db` | `None`; unqualified-table namespace | `--default-db` |
| `products` | `None`; explicit local product-registry path, otherwise discovery | `--products` |
| `parsers` | `None`; default `ParserRegistry()` | `--plugin` factories |
| `sql_engine` | `"athena"` | `--engine` |
| `sql_dialect` | `None`; optional SQL-file override | `--dialect` |
| `scan_commit` | `None`; optional revision provenance override | SDK only |
| `source_provider` | `None`; infer `LocalSource` or `GitHubSource` | SDK only |
| `ref` | `None`; GitHub branch/tag/commit | `--ref` |
| `source_path` | `None`; GitHub-relative file/directory/ZIP | `--path` |

`scan` and the lower-level APIs additionally support `observer`, `log_dir`, `log_level`,
`log_max_bytes` and `log_max_files`. Caller-owned observers must be finalized by callers.
Use `ParserClient(...).run(..., out_dir=...)` when logs and artifacts should share one
run ID; exporting a lower-level result outside its observer creates separate artifact identity.

`DictSchemaProvider` accepts a catalog path or dictionary, or a schema-only mapping
such as `{"raw":{"orders":["order_id","amount"]}}`. It indexes database/table names
case-insensitively, not by distinct engine-qualified identity. Supply the matching catalog.

Explicit Glue lookup, with an optional profile-bound client:

```python
import boto3
from etl_parser.workers.sql import GlueSchemaProvider

session = boto3.Session(profile_name="YOUR_PROFILE", region_name="us-east-1")
schema = GlueSchemaProvider(client=session.client("glue"))
result = client.run("./etl", schema=schema)
```

Without a custom client, `GlueSchemaProvider(region="us-east-1")` uses the normal AWS
credential chain. Lambda `AnalysisConfig.aws_profile` does not configure Glue lookup.

### Source-of-truth schema sources

Available from this release. `etl_parser.schema` exports `SchemaSource` (the protocol),
`GlueSchemaSource`, `PostgresSchemaSource`, `RedshiftSchemaSource`, `SchemaSourceError`
and `write_schema_catalog`; `etl_parser` re-exports them lazily, so importing the package
never imports a database driver.

```python
from etl_parser.schema import (
    GlueSchemaSource, PostgresSchemaSource, RedshiftSchemaSource,
    SchemaSource, SchemaSourceError, write_schema_catalog,
)

glue = GlueSchemaSource(profile="YOUR_PROFILE", region="ap-south-1", databases=["raw"])
postgres = PostgresSchemaSource(schemas=["public"])       # DSN from the environment
redshift = RedshiftSchemaSource(iam=True, profile="YOUR_PROFILE", region="ap-south-1")

write_schema_catalog([glue, postgres], "catalog.json")    # merged, sorted, no credentials
```

A `SchemaSource` answers `columns(dataset_id)` (the existing provider contract),
`catalog()` (the `databases` list, with `datatype`, `description` and `is_partition` per
field) and `relations()` (`from_table`, `to_table`, `from_column`, `to_column`,
`relation_type`, `source: "database"`). A driver that is not installed raises
`SchemaSourceError` naming the extra to install (`.[postgres]`, `.[redshift]`).

**Credentials come from the environment**, never from a keyword that would end up in a
log: `ETL_PARSER_POSTGRES_DSN` or `PGHOST`/`PGPORT`/`PGDATABASE`/`PGUSER`/`PGPASSWORD`,
`ETL_PARSER_REDSHIFT_DSN` or the `REDSHIFT_*` equivalents, and the normal AWS chain for
Glue and for Redshift IAM. There is no `dsn=` password argument on the CLI and no
`--password` option anywhere.

```python
client = ParserClient()
catalog = client.fetch_schema([glue, postgres])           # the merged catalog dict

result = client.run(
    "./etl",
    schema=glue,                 # a SchemaSource, a catalog dict, or a path
    include_code_schema=False,   # default: the source system is the truth
)
result.schema_drift              # {"code_only": {...}, "unused_in_code": [...]}
payload = result.to_dict()       # includes schema_drift
```

`ParserClient.run`/`arun` and `scan` accept `schema=`, `include_code_schema` (default
`False`) and the selection arguments below. With `include_code_schema=False` a table seen
only in code produces an `Unresolved` item of kind `missing_in_source` instead of being
added to `databases`; `True` is the CLI's `--schema-from-code`.

### Choosing what to generate

`generate` selects catalog sections and `databases` selects database names; `None` (the
default) means everything. Unselected sections are passed through unchanged from `prior`.

```python
# Everything, the default.
result = client.run("./etl", prior=prior)

# Only the scripts section, for one database.
result = client.run("./etl", prior=prior, generate=["scripts"], databases=["analytics"])

# Only relations.
result = client.run("./etl", prior=prior, generate=["relations"])

# Databases and scripts for two databases; other sections come from prior.
result = client.run(
    "./etl", prior=prior,
    generate=["databases", "scripts"], databases=["analytics", "marketing"],
)

# The same selection when exporting a saved document directly.
catalog = export_agent_catalog(doc, prior, generate=["scripts"], databases=["analytics"])
```

Valid sections are `databases`, `scripts`, `relations`, `lineage` and `schedules` — the
same names the CLI accepts as `--generate`, with `--database` for the database filter.
Selecting sources (`fetch_schema`, `schema fetch --database`) limits what is read **from**
the source system; `databases` limits what is written **into** the catalog.

## AnalysisConfig reference

Fields below can be passed to `AnalysisConfig(...)` or as a client `config` dictionary.
Unknown fields are rejected. Output/log/source settings are call options, not fields.
SDK configuration does **not** implicitly read provider environment variables; pass
them explicitly. See [CLI environment handling](cli.md#configuration-credentials-and-input-data).

| Field | Default | Meaning |
| --- | --- | --- |
| `ai_lineage` | `"off"` | `off`, `fallback` (diagnostic-selected files), `improve` (eligible selected files) |
| `descriptions` | `False` | Generate missing supported descriptions |
| `background_comparison` | `True` | Piggyback only on a needed description call; never enables AI by itself |
| `dry_run` | `False` | Produce work plan without model calls |
| `runner` | `"lambda-bedrock-invoke"` | `lbi` alias or `anthropic`; `bedrock` is not an option |
| `model` | `None` | Explicit model ID/registered slug for actual calls |
| `lambda_arn` | `None` | Buffered/non-streaming function ARN or name for LBI |
| `region` | `"us-east-1"` | Lambda region |
| `aws_profile` | `None` | Lambda credential profile; otherwise normal credential chain |
| `web_adapter` | `True` | `/bedrock` envelope; `False` selects flat Lambda payload |
| `base_url` | `None` | Required HTTPS root for Anthropic-compatible provider |
| `api_key` | `None` | Secret string/`SecretStr`; excluded from serialization and repr |
| `extra_headers` | `{}` | Optional string dictionary; excluded from serialization and repr |
| `max_calls` | `20` | Range 0–10,000; zero prevents calls |
| `max_output_tokens` | `16000` | Positive per-request cap; must fit the chosen model |
| `max_context_chars` | `60000` | Serialized context bound; 1,024–1,000,000 |
| `max_total_tokens` | `None` | Optional positive conservative token budget |
| `timeout_seconds` | `300` | Positive per-request timeout, maximum 600 |
| `deadline_seconds` | `3600` | Positive AI-stage deadline, after deterministic scanning |
| `include` | `["*"]` | AI file globs; deterministic inventory is not filtered |
| `exclude` | `[]` | AI exclusions; take precedence over inclusions |
| `min_ai_confidence` | `0.0` | Minimum model-reported confidence (0-1) for a proposal to be applied; below it the proposal is recorded as `deferred` with reason `below_confidence_threshold`. CLI: `--min-ai-confidence` |
| `override_existing` | `False` | Regenerate descriptions this package wrote (`description_source` `ai` or `code`); human and verified text is never overwritten |

Do not use `config.model_dump()` as a complete credential-bearing clone: secrets and
headers are intentionally excluded. A normal deep `model_copy(deep=True)` preserves
them in memory. Construct/validate a new `AnalysisConfig` when changing field values.

### Anthropic-compatible runner

```python
import os
from etl_parser import AnalysisConfig, ParserClient

config = AnalysisConfig(
    runner="anthropic",
    base_url="https://gateway.example/api",
    api_key=os.environ["ANTHROPIC_API_KEY"],
    model="YOUR_MODEL",
    extra_headers={},  # or {"x-custom-routing": "example"} if required
    ai_lineage="improve",
    descriptions=True,
    max_output_tokens=16000,
)
client = ParserClient(config=config, log_dir="./logs")
result = client.run("./etl")
# In a backend: result = await client.arun("./etl")
```

Only the selected runner is initialized. The SDK appends `/v1/messages`. Do not put
credentials in URLs; query strings/fragments, transport-owned headers, duplicate
case-insensitive headers and header injection are rejected. No redirects are followed.

### Lambda Bedrock Invoke runner

```python
config = AnalysisConfig(
    runner="lbi",  # normalized to lambda-bedrock-invoke
    lambda_arn="YOUR_NON_STREAMING_FUNCTION",
    model="YOUR_MODEL",
    region="us-east-1",
    aws_profile="YOUR_PROFILE",  # omit for a service IAM role
    web_adapter=True,
    ai_lineage="fallback",
)
client = ParserClient(config=config)
```

Both runners use buffered `complete()` calls. LBI uses `lambda.invoke`, not the
streaming invocation API. If your deployment uses different ARNs for streaming and
non-streaming, supply the non-streaming ARN. There is no streaming option here.

### Selecting modes

```python
# Replace configuration per call; provide provider settings for real AI calls.
plan = client.run("./etl", config=AnalysisConfig(ai_lineage="fallback", dry_run=True))

# Description/background-only work: main lineage remains deterministic.
background_config = AnalysisConfig(
    runner="anthropic", base_url="https://gateway.example/api",
    api_key=os.environ["ANTHROPIC_API_KEY"], model="YOUR_MODEL",
    ai_lineage="off", descriptions=True, background_comparison=True,
)
```

Use `background_comparison=False` for descriptions without background lineage.
Lineage and descriptions share one file request where context permits. Existing
descriptions are preserved and exact identity descriptions can be inherited without
AI. There is no extra audit call when lineage is off. Disabling both AI features
makes zero provider calls. No cross-run answer cache or explicit prompt-cache policy
is implemented; provider-reported cache usage is counted when available.

## Results, errors and logging

`AnalysisRun` exposes `baseline`, `document` (effective lineage), `ai_document`,
`catalog`, `index` (source snapshots), `decisions`, `changes`, `comparison`, `warnings`,
`work`, `status`, and sanitized `configuration`. Client-populated fields are `run_id`,
`metrics`, `artifact_path` and `log_path`. `to_dict()` serializes the source-free
response, using the keys `lineage`, `baseline`, `ai_lineage`, `work_plan`, and the
remaining report/metadata fields. It does not include `index`; lineage expressions
and descriptions can still contain sensitive source-derived information.

```python
if result.status != "success" or result.warnings or result.document.unresolved:
    # Your backend decides whether to queue review, return partial data or fail a job.
    review_items = result.decisions
payload = result.to_dict()
```

There is no SDK `strict=True`: that is CLI exit behavior. Implement your service's
acceptance policy using the result. `partial` is not an exception and does not mean
every mapping failed. `exact`/`inferred` are provenance labels, not measured accuracy.
The deterministic baseline and original diagnostics remain available for review.

Logs default to stderr; disk persistence is optional. Each persisted run has events,
metrics and a completion manifest. No raw prompts/responses, API keys or headers are
logged. Metrics cover throughput, stage latency, source/parser counts, diagnostics,
AI calls/tokens/cache reporting, changes and descriptions; costs/accuracy remain unknown.
See [CLI artifacts and failure handling](cli.md#results-audit-and-recovery).

HTTP 401/403/404/429 stops remaining AI calls in that invocation; unrelated invocations
are not globally paused. Inspect `http_status`, `retry_after_seconds` and
`transport_error_type` where present. There are no implicit paid retries. Return
partial results or schedule a caller-controlled retry after correcting the cause.

## GitHub and custom source providers

```python
result = client.run("https://github.com/OWNER/REPO", ref="main", source_path="etl")
```

Reads use GitHub APIs, not a clone, and pin one resolved revision. `ref`/`source_path`
are GitHub-only unless an explicit source provider handles selection itself.
Advanced limits and authentication:

```python
from etl_parser.sources import GitHubSource

source = GitHubSource(
    "https://github.com/OWNER/REPO", ref="main", path="etl",
    token=os.environ.get("GITHUB_TOKEN"), timeout=30, retries=2,
    max_files=20000, max_file_bytes=5000000, max_total_bytes=100000000,
)
result = client.run("https://github.com/OWNER/REPO", source_provider=source)
```

If `token=None`, GitHubSource uses `GITHUB_TOKEN`, then `GH_TOKEN`; this source-provider
behavior is separate from explicit SDK AI settings. Optional `transport(endpoint)`
injects parsed GitHub responses for testing. The provider exposes `list_files()` and
`scan(extensions=...)`. A custom `SourceProvider` implements the latter and returns
a `ScanIndex`; `LocalSource(path)` is the local implementation. Prefer a fresh provider
per run when managing concurrency. Missing/external dependencies, submodules and size
limits can leave diagnostics; no exhaustive runtime dependency resolution is promised.

## Exports, impact and products: CLI equivalents

```python
from etl_parser import scan
from etl_parser.export.native import read_native, write_native
from etl_parser.export.agent_catalog import export_agent_catalog
from etl_parser.export.openlineage_out import export_openlineage
from etl_parser.graph.builder import LineageGraph
from etl_parser.graph.impact import downstream, upstream
from etl_parser.graph.products import product_dependencies, orchestration_drift

graph = scan("./etl")
document = graph.to_document()
write_native(document, "lineage.json")
loaded = read_native("lineage.json")
graph = LineageGraph(loaded)
catalog = export_agent_catalog(loaded)       # optional second argument: prior catalog dict
events = export_openlineage(loaded)          # JSON-compatible event dictionaries
affected = downstream(graph, "glue://raw/orders", max_depth=2)
origins = upstream(graph, "glue://analytics/order_totals#double_amount")
dependencies = product_dependencies(graph)
drift = orchestration_drift(graph)
```

### What the exported catalog carries

| Key | Meaning |
| --- | --- |
| `databases` | Source-of-truth schema only, unless `include_code_schema=True` adds code-derived entries marked `schema_source: code` |
| `relations` | Relations from a source database (`source: "database"`) or preserved prior human curation |
| `relations_inferred` | Relations inferred from join conditions, `source: "inferred"`, with the contributing `jobs`; never merged into `relations` |
| `schema_drift` | `{"code_only": {"databases": [...]}, "unused_in_code": [{"db_name", "table_name"}]}`, both sorted |
| `scripts[].description` | Falls back to the job docstring's first line with `description_source: code` |
| `lineage.unresolved` | Includes non-gating `missing_in_source`, `analysis_note` and `skipped_entry` items |

Every description records a `description_source` of `human`, `inherited`, `code` or `ai`.
Descriptions and edges an AI stage produced additionally carry `ai_confidence` (the
model's own 0–1 score), `ai_rationale` (a short justification, ≤ 500 characters) and
`ai_model`, alongside the existing `parser="agent_sdk_ai"`, `confidence="inferred"`,
`request_id` and `evidence_digest` provenance. Use `AnalysisConfig.min_ai_confidence` to
defer proposals below a threshold. These are the model's self-reported numbers: treat
them as a triage signal for review, not as measured accuracy.

Impact nodes must exist in the graph; absent IDs raise `ValueError`. Product/DAG
reports reflect only observed and declared evidence, not execution success.
OpenLineage events describe static analysis, not actual ETL runs. For analysis
artifacts use the client's `out_dir`, or low-level `write_analysis(result, directory)`
from `etl_parser.artifacts` (with the observer identity caveat above).

## Legacy description-only SDK

Use this only for enriching an already saved lineage/catalog without source-backed
comparison. It does not alter lineage and does not have the unified file-call budgets.

```python
from etl_parser.describe.client import RunnerConfig, configured_runner, close_runner
from etl_parser.describe.engine import DescriptionEngine

async def enrich_saved(document, catalog):
    runner = configured_runner(RunnerConfig(
        runner="anthropic", base_url="https://gateway.example/api",
        api_key=os.environ["ANTHROPIC_API_KEY"],
    ), timeout=300)
    try:
        engine = DescriptionEngine(runner, model="YOUR_MODEL", max_tokens=16000)
        enriched = await engine.arun(document, catalog, log_dir="./logs")
        return enriched, engine.warnings
    finally:
        await close_runner(runner)
```

`engine.run(document, catalog, ...)` is the sync equivalent. Both accept logging and
optional caller-owned observation. Create an engine per concurrent enrichment job;
its warnings are mutable. The caller owns and closes its runner. Missing supported
descriptions are generated per eligible column, preserving prior descriptions.

## Parser and orchestrator extensions

`ParserRegistry()` includes built-ins; `registry.register(plugin)` adds a named parser.
`ParserRegistry([plugin, ...])` replaces the built-in set. A plugin supplies `name`,
`extensions`, `accepts(source)` and `analyze(source, context) -> WorkerResult`.
Context includes the source index, schema, bindings, default database and SQL settings.
Emit datasets, jobs, table/column edges, schedules, task/job links and unresolved items
using the shared models. Parsers and orchestration handlers share this boundary.
Pass `parsers=registry` to a scan/client call; SDK code does not load CLI `module:factory`
strings automatically. See the [plugin example](../README.md#python-api-and-extensions).

## CLI/SDK differences at a glance

CLI JSON paths are explicitly loaded Python objects in the SDK, CLI environment
precedence is replaced by explicit `AnalysisConfig` values, `--strict` is a caller
acceptance decision, and export/query commands are Python functions. Provider behavior,
parsing, AI policy, evidence validation and lineage preservation are shared.
