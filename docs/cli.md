# CLI usage guide

Use this guide for shell commands and automation. For in-process/backend integration,
see the [Python SDK guide](sdk.md). All URLs, model IDs and function names below are
placeholders, not configured production endpoints.

## Installation and first run

Requires Python 3.11+. From the checkout:

```sh
pip install .
# Optional Glue schema lookups:
pip install '.[glue]'
etl-parser run ./etl --out-dir ./artifacts --log-dir ./logs
```

The first run is deterministic: AI lineage and descriptions are both off.
AI requires your organisation's Agent SDK (`agent-sdk`) from its trusted internal
distribution or source checkout, not an unrelated public package of the same name.
For example, install it from your approved internal checkout with
`pip install /path/to/trusted-agent-sdk`.
Help and deterministic scanning require neither AI credentials nor the private SDK.

## Common workflows

```sh
# Known columns and dynamic environment/table bindings:
etl-parser run ./etl --schema catalog.json --bindings bindings.json --prior catalog.json

# GitHub via read APIs, without cloning:
etl-parser run https://github.com/OWNER/REPO --ref main --path etl --out-dir ./artifacts

# Plan AI work without making model calls:
etl-parser run ./etl --ai-lineage fallback --descriptions --dry-run

# Anthropic-compatible gateway. Set ANTHROPIC_API_KEY securely first.
etl-parser run ./etl --runner anthropic \
  --base-url https://gateway.example/api --model YOUR_MODEL \
  --ai-lineage improve --descriptions --max-output-tokens 16000 \
  --log-dir ./logs --out-dir ./artifacts

# Only problem files get AI lineage; descriptions remain off:
etl-parser run ./etl --runner anthropic \
  --base-url https://gateway.example/api --model YOUR_MODEL \
  --ai-lineage fallback --no-descriptions --include '*.py'

# Description + same-call background comparison; main lineage stays deterministic:
etl-parser run ./etl --runner anthropic \
  --base-url https://gateway.example/api --model YOUR_MODEL \
  --ai-lineage off --descriptions --background-comparison

# Description only, without a background comparison:
etl-parser run ./etl --runner anthropic \
  --base-url https://gateway.example/api --model YOUR_MODEL \
  --descriptions --no-background-comparison

# Lambda: use the non-streaming function ARN/name; lbi is a valid alias.
etl-parser run ./etl --runner lbi --lambda-arn YOUR_FUNCTION \
  --model YOUR_MODEL --aws-profile YOUR_PROFILE --ai-lineage fallback
```

Optional headers: `--extra-headers '{"x-custom-routing":"example"}'` or
`--extra-headers-file headers.json`, never both. Keys and values must be strings;
`--extra-headers '{}'` clears configured headers. No custom headers are added by
default. HTTPS roots cannot contain embedded credentials, query parameters or
fragments; redirects are not followed. The SDK appends `/v1/messages`.

## Configuration, credentials and input data

`run --config settings.json` accepts the fields documented in
[AnalysisConfig](sdk.md#analysisconfig-reference). Unknown fields fail validation.
Do not store credentials in a committed configuration file. Minimal example:

```json
{
  "runner": "anthropic",
  "base_url": "https://gateway.example/api",
  "model": "YOUR_MODEL",
  "ai_lineage": "fallback",
  "descriptions": false,
  "max_output_tokens": 16000,
  "timeout_seconds": 300,
  "deadline_seconds": 3600
}
```

Precedence for `run`: explicit CLI option, then its environment variable (if any),
then configuration file, then defaults. Header flags replace the configured header
object. Legacy `describe` has no `--config` option.

| Environment variable | Option / purpose |
| --- | --- |
| `ETL_PARSER_RUNNER` | `--runner` |
| `ETL_PARSER_MODEL` | `--model` |
| `ETL_PARSER_LAMBDA_ARN` | `--lambda-arn` |
| `ANTHROPIC_API_BASE_URL`, then `ANTHROPIC_BASE_URL` | `--base-url`; first has precedence |
| `ANTHROPIC_API_KEY` | `--api-key`; prefer this to a visible command argument |
| `AWS_REGION` | `run`/`describe --region` |
| `AWS_PROFILE` | `run`/`describe --aws-profile` for the Lambda runner |
| `GITHUB_TOKEN`, then `GH_TOKEN` | GitHub source access; never embed tokens in URLs |

`--aws-profile` selects a Lambda credential profile; it does not log in or run SSO.
Glue lookup uses its own normal AWS credential chain. Use the SDK for an explicitly
profile-bound Glue client. GitHub reads may need a repository-scoped token.

Start with [catalog.template.json](../catalog.template.json).
`--schema` supplies physical column names; `--prior` preserves descriptions/metadata.
Use both when both are needed. Native lineage JSON is a different format.
A schema-only mapping also works: `{"raw":{"orders":["order_id","amount"]}}`.
Bindings are string-to-string JSON, e.g. `{"env:ENV":"prod"}`. Unknown dynamic
names remain diagnostics; the framework does not guess runtime values.

## Complete command reference

`etl-parser` and `python -m etl_parser` are equivalent. Every command/group supports
`--help`, including commands with required positional arguments. Get help without
credentials or network calls:

```sh
etl-parser --help
etl-parser run --help
etl-parser scan --help
etl-parser describe --help
etl-parser export --help
etl-parser export catalog --help
etl-parser export openlineage --help
etl-parser impact --help
etl-parser products --help
```

Top-level shell helpers: `--show-completion` prints shell-completion setup;
`--install-completion` installs it. No command starts a live AI request just to show help.

### All command forms

| Command | Positional arguments | Purpose and result |
| --- | --- | --- |
| `run SOURCE` | Local path or GitHub repository URL | Deterministic analysis plus enabled AI stages; unique artifact directory and stdout summary |
| `scan REPO` | Local path or GitHub repository URL | Deterministic-only native JSON; no LLM |
| `describe LINEAGE` | Saved native lineage JSON | Enrich an existing catalog through the chosen SDK runner |
| `export catalog LINEAGE` | Saved native lineage JSON | Agent catalog JSON, optionally preserving prior metadata |
| `export openlineage LINEAGE` | Saved native lineage JSON | Directory of static OpenLineage event JSON files |
| `impact LINEAGE NODE` | Saved native JSON and canonical dataset or `dataset#column` | Downstream by default; `--upstream` reverses traversal |
| `products LINEAGE` | Saved native lineage JSON | Observed/declared product dependencies and orchestration drift |

```sh
etl-parser run ./etl --out-dir ./artifacts
etl-parser scan ./etl --out lineage.json
etl-parser describe lineage.json --catalog catalog.json --out enriched.json \
  --runner lbi --lambda-arn MY_FUNCTION --model MY_MODEL
etl-parser export catalog lineage.json --prior catalog.json --out catalog.updated.json
etl-parser export openlineage lineage.json --out ./openlineage-events
etl-parser impact lineage.json 'glue://raw/orders' --depth 2
etl-parser impact lineage.json 'glue://analytics/order_totals#double_amount' --upstream
etl-parser products lineage.json
```

### Where schema comes from, and what `--prior` does

The **source system is the truth** for databases, tables, columns, datatypes and
partition keys. Code only tells the parser how data is used. So by default the
`databases` section contains only what a schema source or `--prior` supplied:

- A table referenced in code but absent from the source schema is **not invented**. It
  becomes an `Unresolved` item of kind `missing_in_source`, which is informational and
  never fails the command.
- `--schema-from-code` (on `run` and `scan`) restores the older behaviour of adding
  code-derived tables and columns, each marked `schema_source: code`.

`--prior catalog.json` merges into an existing catalog rather than replacing it:

- Databases are matched **by `db_name` first**. When exactly one prior database has that
  name it is merged into regardless of its `db_type`, so a `sqlite`, `postgres` or
  `mysql` catalog keeps its metadata. New databases are never written as `db_type: glue`
  (they use `athena`).
- Scripts are matched by `job_id`, then exact `script_path`, then `script_name`, then
  path suffix. Prior scripts that this scan did not touch are kept, unchanged.
- Human descriptions, flags and other caller metadata are preserved; sections you did
  not select with `--generate` are passed through untouched.

Every description carries a `description_source`:

| `description_source` | Meaning |
| --- | --- |
| `human` | Text already in the prior catalog with no recorded source |
| `inherited` | Copied from an exact identity column upstream, no model involved |
| `code` | Taken from the job's docstring or SQL comment first line |
| `ai` | Generated by an AI stage; carries `ai_confidence`, `ai_rationale` and `ai_model` |

`relations` holds relations that came from a source database (`source: "database"`) or
from prior human curation. Relations the parser infers from join conditions are written
to a separate top-level `relations_inferred` list, marked `source: "inferred"` with the
contributing `jobs`. They are never merged into `relations`: a join in a query is
evidence of a relationship, not a declared constraint.

`schema_drift` is a separate top-level block comparing code against the source schema:

```json
{
  "schema_drift": {
    "code_only": {"databases": [{"db_name": "analytics", "db_type": "athena", "tables": []}]},
    "unused_in_code": [{"db_name": "raw", "table_name": "legacy_orders"}]
  }
}
```

`code_only.databases` uses the exact shape of the `databases` section, so an entry can be
copied across once it has been verified against the real source system. `unused_in_code`
lists source tables that no scanned job reads or writes. Both lists are sorted, and the
counts are also emitted as one `schema.drift` log event.

### Fetching schema from the source system

`etl-parser schema fetch` reads databases, tables, columns, datatypes, partition keys and
relations straight from the source and writes them as a catalog you can pass to
`--schema` or `--prior`. Available from this release.

```sh
# Glue, limited to two databases, into catalog.json.
etl-parser schema fetch --glue --database raw --database analytics \
  --aws-profile YOUR_PROFILE --region ap-south-1 --out catalog.json

# Postgres, credentials from the environment only.
export ETL_PARSER_POSTGRES_DSN='postgresql://HOST:5432/DBNAME'   # or PGHOST/PGDATABASE/...
etl-parser schema fetch --postgres --schema-name public --out catalog.json

# Redshift with IAM authentication through a named profile.
etl-parser schema fetch --redshift --iam --aws-profile YOUR_PROFILE \
  --region ap-south-1 --schema-name analytics --out catalog.json

# Several sources merged into one catalog.
etl-parser schema fetch --glue --postgres --out catalog.json
```

| Option | Meaning |
| --- | --- |
| `--glue` | Read the AWS Glue Data Catalog; needs the `glue` extra |
| `--postgres` | Read PostgreSQL `information_schema`, comments and keys; needs the `postgres` extra |
| `--redshift` | Read Redshift `svv_columns`/`svv_table_info` and constraints; needs the `redshift` extra |
| `--database NAME` | Restrict to this source database; repeatable, defaults to all |
| `--schema-name NAME` | Restrict to this Postgres/Redshift schema; repeatable, defaults to all |
| `--iam` | Use IAM authentication for Redshift, via `--aws-profile`/`--region` |
| `--aws-profile TEXT` | Named AWS profile for Glue and Redshift IAM |
| `--region TEXT` | AWS region for Glue and Redshift IAM |
| `--out PATH` | Destination catalog JSON; parent directories are created |

**Credentials are read from the environment only.** There is deliberately no `--dsn` and
no `--password` option, so a connection secret never appears in shell history, a process
list or a CI log:

| Source | Environment |
| --- | --- |
| Glue | Normal AWS credential chain, or `--aws-profile`/`AWS_PROFILE` |
| Postgres | `ETL_PARSER_POSTGRES_DSN`, or `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` |
| Redshift | `ETL_PARSER_REDSHIFT_DSN`, or the `REDSHIFT_*` equivalents; `--iam` uses the AWS chain instead |

The emitted catalog is sorted for determinism and contains no credential in any form.
`scan`/`run --glue` use the same Glue source inline, honouring `--aws-profile`.

### Choosing what to generate

`scan`, `run` and `export catalog` accept two repeatable selection flags. Both default
to "everything", so omitting them keeps the previous behaviour.

| Flag | Meaning |
| --- | --- |
| `--generate SECTION` | Catalog section to rebuild: `databases`, `scripts`, `relations`, `lineage` or `schedules`. Repeat for several; omitted means every section |
| `--database NAME` | Restrict generation to this database; repeat for several; omitted means every database |

An unknown section name is a usage error (exit code 2). Sections you do not select are
**passed through unchanged from `--prior`**, so a partial regeneration never drops the
descriptions, flags or relations an earlier catalog already carried. Without `--prior`
an unselected section is simply left empty.

```sh
# Everything, the default.
etl-parser export catalog lineage.json --prior catalog.json --out catalog.updated.json

# Only the scripts section, and only for one database.
etl-parser export catalog lineage.json --prior catalog.json --out catalog.updated.json \
  --generate scripts --database analytics

# Only relations (for example after adding foreign keys in the source database).
etl-parser export catalog lineage.json --prior catalog.json --out catalog.updated.json \
  --generate relations

# Databases and scripts for two databases; schedules/lineage/relations come from --prior.
etl-parser export catalog lineage.json --prior catalog.json --out catalog.updated.json \
  --generate databases --generate scripts --database analytics --database marketing

# The same selection while scanning or running.
etl-parser scan ./etl --generate scripts --database analytics --out lineage.json
etl-parser run ./etl --prior catalog.json --generate scripts --database analytics
```

The source-side equivalent is `etl-parser schema fetch --database NAME`, which limits
which databases are read **from** Glue, Postgres or Redshift; `--database` here limits
which databases are written **into** the catalog. Use both when a large estate should be
refreshed one database at a time.

### Source and schema options (`run`, `scan`)

| Option | Default / behavior |
| --- | --- |
| `--schema PATH` | Optional JSON catalog or schema-only mapping; mutually exclusive with Glue lookup |
| `--glue` / `--no-glue` | Off by default; explicitly fetch schemas from AWS Glue |
| `--schema-from-code` | Off by default; also add tables/columns discovered only in code, marked `schema_source: code` |
| `--bindings PATH` | JSON string-to-string substitutions; defaults to none |
| `--products PATH` | Optional explicit local product registry file; otherwise discover repository metadata |
| `--default-db TEXT` | Default namespace for unqualified table names |
| `--engine TEXT` | `athena`; table namespace/executing SQL engine |
| `--dialect TEXT` | Optional SQL-file dialect override |
| `--plugin MODULE:FACTORY` | Repeatable trusted installed parser/orchestrator factories |
| `--ref TEXT` | GitHub branch/tag/commit, resolved once; GitHub-only |
| `--path TEXT` | GitHub-relative file/directory/ZIP selection; GitHub-only |
| `--region TEXT` | AWS region; `run` also uses this for Lambda, `scan` uses it for explicit Glue lookup |

`scan --out PATH` defaults to `lineage.json`. `run --out-dir PATH` defaults to `artifacts`.
`run --prior PATH` preserves an existing catalog. Schema/product/binding/prior inputs
remain explicit local JSON/YAML configuration paths even when scanning GitHub source.
GitHub limits and advanced provider configuration are documented above and in the Python API.

### AI/run policy options (`run`)

| Option | Default / behavior |
| --- | --- |
| `--config PATH` | Optional JSON `AnalysisConfig`; unknown fields rejected |
| `--ai-lineage off\|fallback\|improve` | `off`; only explicit fallback/improve can add main lineage |
| `--descriptions` / `--no-descriptions` | Off; generate missing supported descriptions |
| `--background-comparison` / `--no-background-comparison` | Allowed by default only within a needed description call; never enables AI alone |
| `--dry-run` / `--no-dry-run` | Off; enabled means deterministic scan/work plan and zero model calls |
| `--max-calls INTEGER` | 20; zero means no calls; upper validation bound 10,000 |
| `--max-output-tokens INTEGER` | 16,000 per response; must also fit the selected model's limits |
| `--max-context-chars INTEGER` | 60,000 serialized context characters; valid range 1,024–1,000,000 |
| `--max-total-tokens INTEGER` | Optional conservative reservation/accounting budget; not a provider billing guarantee |
| `--timeout-seconds NUMBER` | 300 per model request; maximum 600 |
| `--deadline-seconds NUMBER` | 3,600 for the AI stage; deterministic scan time is separate |
| `--min-ai-confidence FLOAT` | Unset; 0–1 threshold below which an AI proposal is deferred rather than applied |
| `--override-existing` | Off; regenerate descriptions this tool wrote (`description_source` `ai` or `code`). Text marked `human` or `verified` is never overwritten. Available on `run` and `describe` |
| `--include GLOB` | Repeatable AI file inclusions; defaults to `*` |
| `--exclude GLOB` | Repeatable AI file exclusions; exclusions win |
| `--strict` / `--no-strict` | Off; fail for unresolved/incomplete work instead of only reporting partial status |

The source still gets deterministic analysis even if an AI filter excludes it.
`--max-calls` is a request limit, not a target: inheritance/skips can mean fewer calls.

### Runner options (`run`, `describe`)

| Option | Default / behavior |
| --- | --- |
| `--runner lambda-bedrock-invoke\|lbi\|anthropic` | `lambda-bedrock-invoke`; `lbi` is its alias |
| `--model TEXT` | Required for actual AI work; provider-specific ID/registered slug, no hard-coded model |
| `--lambda-arn TEXT` | Function name/ARN for Lambda Bedrock Invoke only |
| `--aws-profile TEXT` | Optional AWS profile for Lambda; omitted uses credential chain |
| `--region TEXT` | `us-east-1` unless configured; Lambda region |
| `--web-adapter` / `--no-web-adapter` | On; `/bedrock` adapter envelope vs flat `{modelId,payload}` Lambda handler |
| `--base-url HTTPS_URL` | Required Anthropic-compatible root; no silent public-endpoint fallback |
| `--api-key TEXT` | Anthropic key; prefer `ANTHROPIC_API_KEY` environment variable |
| `--extra-headers JSON` | Optional custom header string object; empty by default |
| `--extra-headers-file PATH` | Optional JSON header file instead of inline JSON |

Current calls are **non-streaming** `complete()` requests. LBI uses buffered Lambda
`invoke` and the `/bedrock` route with the Web Adapter enabled—not
`invoke_with_response_stream` or `/bedrock_stream`. Supply the buffered function ARN
if your deployment has separate buffered/streaming functions. No `--stream` or separate
`--stream-lambda-arn` option is implemented.

### Description/export/query options

| Command | Command-specific options |
| --- | --- |
| `describe` | Required `--catalog PATH`, `--out PATH`, `--model`; shared runner settings; `--max-tokens` defaults to 16,000 per column |
| `export catalog` | Required `--out PATH`; optional `--prior PATH` to preserve metadata |
| `export openlineage` | Required `--out DIRECTORY`; creates directory and writes one event per job |
| `impact` | `--upstream` (otherwise downstream); optional `--depth INTEGER` ≥ 0; no limit when omitted |
| `products` | No extra policy options; reads native product/DAG evidence |

Legacy `describe` has no repository source context, combined lineage stage, or unified
run budgets/strictness. Use `run` for those controls. Its provider failures leave affected
descriptions unchanged and emit warnings; an empty description is not fabricated.

### Logging and exit behavior

All seven leaf commands accept `--log-dir DIRECTORY` (optional persistence) and
`--log-level DEBUG|INFO|WARNING|ERROR|CRITICAL` (console default INFO). `run`, `scan`,
and `describe` additionally accept `--log-max-bytes` (10,000,000) and `--log-max-files`
(20). File logs retain DEBUG events independently of console verbosity.

| Exit code | When |
| --- | --- |
| `0` | The command finished; diagnostics may still be present in the JSON summary |
| `1` | An `Unresolved` item of kind `unsupported_syntax` remains, or `--strict` was set on `run` and any unresolved item, warning or non-success status remains |
| `2` | Usage error (unknown option, missing input file, bad value), **or** the AI stage failed provider authentication/authorization (HTTP 401/403) |

Only `unsupported_syntax` gates. The informational kinds `analysis_note`,
`skipped_entry` and `missing_in_source` never change the exit code, so CI can gate on
parser coverage without failing on heuristic notes or tables that are absent from the
source schema. Exit code 2 for an authentication failure does not need `--strict`: a
rejected credential is a configuration error, not a partial result.

`run` writes useful partial results and reports status unless `--strict` is set.
Command/configuration failures are nonzero. Inspect warnings, diagnostics and manifests,
not just the exit code. JSON summaries/results go to stdout, logs to stderr.

### Requests and prompt caching

Source-backed AI analysis makes at most one attempted request per selected file per run,
sharing lineage/descriptions when requested. Legacy descriptions can make one request
per eligible column. Both paths log SDK input/output and cache-read/write usage when
reported. There is no local answer cache, cross-run response cache, or explicit prompt
cache configuration in this release; no cache tuning was added for the user's question.
Provider/gateway-side caching may still occur and is not guaranteed by selecting a runner.

Supported OpenAI models enable prompt caching by default; matching prefixes, model
rules and provider settings determine reuse. That does not prove an Anthropic-compatible
gateway forwards the same options/accounting. See the
[official prompt-caching guide](https://developers.openai.com/api/docs/guides/prompt-caching).
Zero SDK cache fields can mean no cache use or missing gateway reporting; do not infer
upstream billing from them alone. Each cached request still generates a new answer.


## Results, audit and recovery

`run` produces a unique `run_<id>/` directory containing `lineage.json`,
`lineage.deterministic.json`, `catalog.json`, `decisions.json`, `changes.json`,
`work-plan.json` and `manifest.json`. When a comparison exists, it also writes
`lineage.ai.json` and `lineage.comparison.json`. The manifest is written last;
missing completion markers indicate interrupted output. AI-off never replaces
main lineage with background proposals.

Optional log directories contain per-run `events.jsonl` (rotated when needed),
`metrics.json` and `manifest.json`. Metrics cover source/parser counts,
diagnostics, stage/request durations, selected/skipped files, AI calls/tokens,
cache reporting, accepted/rejected changes, descriptions and artifact bytes.
These are operational metrics, not measured accuracy or guaranteed dollar cost.
Raw prompts/responses and credentials are not logged; exported lineage expressions
remain source-sensitive.

HTTP 401/403/404/429 stops subsequent AI calls within that run. Inspect
`http_status`, `retry_after_seconds` and `transport_error_type` when present.
Timeouts, `output_token_limit`, `response_schema_invalid`, `call_limit` and
`deadline_exceeded` are distinct findings. No automatic paid retry occurs.
Correct the issue, then rerun selected files with `--include`. This is not an
automatic persisted resume; previous catalogs can be supplied with `--prior`.

SQL, PySpark, Pandas, Polars, local ZIP helpers and Airflow are analyzed statically.
GitHub and local scanning use the same parsers. Unknown columns, unresolved names,
unsupported code, external helpers, or incomplete DAG links remain visible.
Accepted AI additions are inferred; exact baseline edges are preserved. Review
diagnostics and proposal decisions rather than treating exit code zero as full accuracy.

For optional performance and explicitly paid diagnostic commands, see
[development and live checks](../README.md#development-and-explicit-live-checks).
