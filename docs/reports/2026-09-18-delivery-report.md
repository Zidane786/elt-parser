# ETL parser delivery report

Date: 18 September 2026. Baseline before this extension: `a218033`.

Follow-up: [runner options and actual live gateway evaluation](2026-09-18-gateway-validation.md)
records the subsequent Anthropic/LBI request, expanded README/catalog template,
live partial results, local fixes and remaining approval blocker. Counts and the
"no live model calls" statement below describe the earlier `81f569d` release only.

## In simple language

Your framework reads ETL code and builds a map of which tables and columns feed
other tables and columns. Its normal operation uses our parsing logic, not an LLM.
It does not run the ETL jobs themselves.

The new release adds optional AI review, shared AI descriptions, detailed logs and
metrics, and GitHub scanning without cloning. It keeps the deterministic result
separate so you can see what our parser found and what AI proposed or added.

Both AI lineage and AI descriptions are **off by default**. With descriptions on
but AI lineage off, background AI lineage is obtained only inside the same needed
description call. There is no separate paid call just to compare the two maps,
and that background result cannot replace the main lineage.

The requested core workflows are implemented and tested. This does **not** mean
every possible ETL program can be understood perfectly. Unsupported or uncertain
code remains visible as diagnostics. Live AWS/model quality and production-scale
capacity are not established by the local tests.

## Every request and what was delivered

| Your request | Delivery and important boundary |
| --- | --- |
| Finish the original parser plan | The original 11-step implementation was reviewed and completed within its static-analysis scope; see the existing implementation review. This pass adds the AI/logging/GitHub extension. |
| Explain what the framework does | README examples, this report, design notes, and a checked implementation plan describe the flow and responsibilities. |
| Track actual tables and columns, not aliases | SQL scopes/CTEs and Python frames resolve physical identities before emitting supported mappings; unresolved/ambiguous references remain diagnostic. |
| Handle dynamic names such as `f"table_{env}"` | Static string folding and explicit bindings resolve known values. Diagnostics retain expression, symbols, source/line, assumptions and remediation for unknown runtime values. Arbitrary environments are not invented. |
| Support SQL, PySpark, Pandas and Polars | Implemented static workers and regression fixtures cover common reads, writes, projections, joins, expressions, grouping and other operations listed below. Not every API/runtime behavior is modeled. |
| Respect DAGs and dependencies | Airflow task order and observed data flow are separate dependency evidence; graph/impact/product views use both. |
| Make parsers and orchestrators extendable | `ParserRegistry` and `WorkerResult` provide the extension boundary. New orchestrators can emit schedules/task links without rewriting graph exports. |
| Scan a repository, path or file | Local repository/directory/file/ZIP entry modes share one index. Single-file scans can follow sibling helpers. |
| Inspect uploaded custom Python ZIP modules in the repo | ZIP sources are indexed in memory with member/size/path checks; no extraction or execution. Imported helper calls can be traced. |
| Connect to the supplied repository and push work | Work targets `Zidane786/elt-parser`, branch `main`. Release commit/push is verified separately in the final handoff. |
| Explain the LLM's role | Deterministic parsers own normal lineage. AI is optional for descriptions and explicitly enabled lineage proposals. Application policy—not the model—decides scope and acceptance. |
| Replace direct LLM clients with the Agent SDK Lambda invoke client | Both description paths use the private `gdtc-agent-sdk` `BedrockInvokeLambdaRunner`; the base parser has no mandatory private SDK dependency. |
| Independently control descriptions and lineage | `--ai-lineage off/fallback/improve`, `--descriptions/--no-descriptions`, and `--background-comparison/--no-background-comparison`. |
| Improve deterministic lineage using AI | `improve` reviews selected ETL files. Validated gap additions are inferred, exact deterministic mappings stay intact, and conflicting proposals are deferred. |
| AI only when the framework needs a second look | `fallback` selects files with diagnostics, partial mappings or input/output jobs lacking column edges. It does not send the entire repository to the model. |
| Share lineage and description calls | Source-backed `run` requests both in one structured response per eligible file where enabled. No implicit paid retry or repair call. |
| Background disagreement only inside a needed description call | Implemented and tested: descriptions off means no background AI; existing/inherited descriptions mean no audit call; oversized context drops comparison, not adds another request. |
| Separate AI lineage and validate differences | `lineage.ai.json` and `lineage.comparison.json` are separate. Comparisons cover table/column signatures and textual transformation differences, with incomplete/assisted review labels. |
| Log normal runs and all decisions/control transfers | Structured events identify framework/parser/policy/SDK actors, IDs, stages, findings, decisions and failures. The framework logs analysis events, not executed database transactions. |
| Console default plus optional chosen log folder | JSON events go to stderr by default. `--log-dir` creates unique event/metrics/manifest files; stdout remains machine-readable. |
| Metrics to improve parser and AI performance | Source/parse/graph counts, diagnostics, timing/throughput, API retries, SDK usage, change decisions and comparison counts. Unknown cost/accuracy stay unknown. |
| Choose model, AWS profile and Lambda ARN | CLI/config/environment controls for model, ARN/name, region, profile and SDK envelope. Profile selection is not automatic login/SSO. |
| Read GitHub through APIs without cloning | Commit-pinned tree/blob reads feed the same parsers, including in-repo ZIPs/helpers/DAGs/product metadata. API limits and unavailable external objects are explicit. |
| Write all discussion into the Superpowers plan | Discussion preserved in the design spec, actual task completion recorded in the follow-up plan. No unavailable Superpowers skill is claimed to have run. |
| Make it robust and provide a simple final report | Expanded regression/adversarial tests, isolated base environment, build/lint checks, real-folder verification, live GitHub parity, benchmark, and this report. Reliability limits are explicit. |

“Image generation” was interpreted as lineage generation because the rest of the
request described table/column maps. AI-generated bitmap pictures were not added.

## How the process works

1. Read a bounded local or pinned GitHub source snapshot; do not execute its code.
2. Run deterministic SQL/Python/Airflow analysis and build the baseline graph.
3. Preserve/inherit descriptions and decide which files, if any, need enabled AI work.
4. Apply filters, context/call/token/time budgets, then ask the configured SDK runner.
5. Validate response structure, source evidence, scope and available schema columns.
6. Compare proposals with the baseline; policy records accepted/deferred/rejected/no-op changes.
7. Write the effective graph, unchanged baseline, catalog and separate AI/audit artifacts.

The model has no tools. It cannot choose arbitrary files, increase its budget, execute
source code, or decide to overwrite exact lineage. A matching source quote establishes
that evidence exists; it does not mathematically prove that the AI's interpretation is right.

## Supported analysis

| Area | Examples of implemented support |
| --- | --- |
| SQL | CTAS/views, INSERT SELECT target positions, aliases/CTEs, unions, MERGE branches/subqueries, UPDATE FROM, session temporary tables, schema expansion, predicate dependencies |
| PySpark | Table/file reads and writes, select/alias, computed/renamed columns, joins, grouping/aggregates, unions, filters, windows, SQL expressions, common scalar operations |
| Pandas | SQL/file reads, connection dialects, projections/assignments, rename, merge/join, common grouping/aggregation, writes |
| Polars | File/lazy reads, select/expressions/aliases, with_columns, joins, group_by/agg, filters, writes |
| Python names/helpers | Constants, bounded loops, f-strings/formatting/concatenation, explicit environment bindings, bounded local imports/function helpers, ZIP modules |
| Airflow | DAG constructors/contexts/decorators, task decorators, Python/Bash/SQL operators, list/shift/chain dependencies, task-only bridges, locally resolvable cross-DAG references |
| Products | Product YAML metadata, database ownership, schedules, declared/observed product dependencies and orchestration drift |
| Output | Native lineage JSON, agent catalog, synthetic static OpenLineage events, table/column impact traversal |
| Sources | Local directory/file/ZIP and GitHub repository/ref/subpath using the same index and parsers |

Same-spelled tables in different engines remain distinct unless there is explicit
alias evidence. Output aliases are retained as actual output names; source aliases
are traced back to physical inputs where possible.

## How descriptions are generated

Existing descriptions are preserved. Exact identity columns can inherit an upstream
description without an LLM. Remaining supported target columns can be described from
their transformations and upstream descriptions, through the configured SDK.

There are two entry points:

- `describe lineage.json`: legacy per-column description enrichment from saved evidence.
  It lacks source snapshots and does not perform a background source audit.
- `run SOURCE --descriptions`: source-backed structured analysis, grouped by file.
  It can return descriptions and shadow lineage together. When explicit AI lineage
  is enabled, the same response can propose main-graph gap additions too.

Descriptions must concern requested missing columns and be compatible with the selected
mapping. Partial mappings need accepted AI support before receiving an AI description.
Conflicting descriptions are rejected. No automatic extra call is made to repair them.

## Logs, artifacts and metrics

`--log-dir ./logs` creates a UTC/run-ID folder containing `events.jsonl` (and rotated
files), `metrics.json`, and `manifest.json`. Rotation defaults to 10 MB per event file
and 20 files; existing logs are never silently deleted. Run-directory permissions
are owner-private. Disk/rotation failures mark the audit incomplete and prevent
subsequent AI work; a failure during a file's AI processing rolls back its mutations.

`--out-dir ./artifacts` creates a separate unique folder containing:

- `lineage.json`: effective main lineage.
- `lineage.deterministic.json`: baseline before AI additions.
- `catalog.json`: catalog with permitted descriptions.
- `work-plan.json`, `decisions.json`, `changes.json`: what was selected and decided.
- `lineage.ai.json`, `lineage.comparison.json`: only when AI review produced a comparison/candidates.
- `manifest.json`: final atomic completion marker, source/config and content digests.

| Measurement | Meaning / use |
| --- | --- |
| Inventory/read/parse counts and bytes | Find excluded, unsupported, failed or unexpectedly missing inputs |
| Imports/ZIP members and rejections | Debug helper resolution and unsafe/oversized archives |
| Raw findings vs final graph counts | Understand deduplication and graph construction |
| Confidence/diagnostic categories | Prioritize unsupported constructs and missing schemas; not an accuracy score |
| Per-stage/API/model duration | Locate slow scanning, parsing, networking or AI calls |
| Recent p95 latency | Bounded sample of the latest 256 stage timings, not an all-history percentile |
| Whole-run throughput | Files/indexed bytes divided by run duration, not isolated parser/model speed |
| API requests/retries/waits | Diagnose GitHub availability and rate-limit overhead |
| AI attempts/completions/failures and usage | Observe request behavior, reported tokens and missing usage reporting |
| Accepted/rejected/deferred/unchanged/rollback counts | Understand validation and merge-policy outcomes; decisions may be proposed before rollback |
| Description outcomes | See preserved/inherited/generated/rejected/unfilled work (counter set varies by entry point) |
| Table/column comparisons | Identify review disagreements and textual transformation differences, not ground truth |
| Budget skips and final status | Distinguish completed, partial, failed and cancelled processing |

Cost is not estimated without pricing data. Accuracy is not estimated without independent
labels. No cache-hit benefit, cost savings or production throughput is fabricated.
No raw prompts, model responses, source payloads, exception messages or credentials
are deliberately written to event logs. Redaction is best-effort; paths and dataset
names remain sensitive. Graph/change artifacts do contain expressions and must be
protected as source-sensitive data. Enabling AI sends selected context to the configured
service; this is distinct from logging and may incur charges.

## Verification evidence

### Automated checks

- Full local suite with trusted private Agent SDK 1.3.1: **173 tests passed**.
- Isolated base suite without that SDK: **135 passed, two SDK-dependent modules skipped**.
- Ruff and `git diff --check`: passed.
- Wheel and source distribution: built successfully offline.
- Both SDK Lambda envelopes and profile forwarding are tested with mocked transport.
- No live Lambda, Bedrock/model, or Glue request was made.

The added tests cover defaults, all six stage combinations, same-call reuse, inherited
descriptions, fallback scope, immutable baseline, exact conflicts, malformed responses,
source/digest/line/job/schema validation, budget/timeout behavior, audit rollback,
console isolation, planted secrets, log rotation/disk/close failures, cancellation,
threaded logging, GitHub pinning/truncation/HTTP errors/limits, and safe ZIP handling.

Tests exercise the implemented boundaries; they do not prove universal correctness or
production credentials/model compatibility. SDK-dependent CI tests require installing
the trusted private SDK; the public/base workflow explicitly skips those modules.

### Your actual ETL folder

Rechecked `/Users/zidanesunesara/Desktop/Projects/de_agent/test_data/etl`, using its
matching sibling `catalog.json` (not the unrelated `de_agent/schema/catalog.json`).

| Result | Count |
| --- | ---: |
| Jobs | 28 |
| Datasets | 42 |
| Table links | 35 |
| Column links | 177 |
| Exact column links | 165 |
| Partial column links | 12 |
| Remaining diagnostics | 8 `unknown_column` |
| AI work items with defaults | 0 |

All 28 exported script records equal the reviewed golden catalog. The original catalog
missed five reads and had stale schedule strings; the original fixture was preserved
and reviewed corrections were frozen separately. The remaining partial/UDF/computed
cases are not silently declared solved. “165 exact” is a parser confidence count,
not a claim of measured 93.2% accuracy.

### Live GitHub without a clone

Read `https://github.com/Zidane786/elt-parser.git` through GitHub tree/blob APIs,
at commit `a218033ab20e98d6b69e923a537d7b313ae82eb1`, subpath `tests/fixtures/etl`.
The complete native document equaled the local fixture document when both used the
same revision and schema: 28 jobs, 177 column links. No checkout/worktree was created.

This is a real read-only integration check. Large/private/Enterprise repository
configurations and all possible rate-limit conditions were not live-tested; error
and limit behavior is covered with controlled API fixtures.

### Repeatable performance probe

Command: `python -m benchmarks.scan_benchmark --synthetic-files 500 --repeats 3`.
Local Python 3.12.12, macOS ARM64, with tracemalloc enabled:

| Mode | Median time | Peak traced Python allocations | Jobs/second |
| --- | ---: | ---: | ---: |
| Console ERROR, no persistent event file | 2.427 s | 25.73 MB | 205.97 |
| Detailed persistent event file | 2.524 s | 24.21 MB | 198.12 |

Both modes produced identical 500-job, 1,000-dataset, 500-table-edge,
1,000-column-edge graphs with zero diagnostics. Persistent logging took about
1.04 times the median duration in this small local sample. Both modes still
instrument events. This is not a disabled-instrumentation comparison, randomized
capacity study, RSS measurement, AI latency benchmark or production guarantee.

## Useful commands

```sh
# Default deterministic work, with persisted debugging information.
etl-parser run ./etl --schema catalog.json --out-dir ./artifacts --log-dir ./logs

# Find which files fallback would review, without any AI call.
etl-parser run ./etl --ai-lineage fallback --dry-run --out-dir ./artifacts

# Enable bounded AI fallback plus shared descriptions.
etl-parser run ./etl --ai-lineage fallback --descriptions \
  --lambda-arn YOUR_FUNCTION --model YOUR_MODEL --aws-profile YOUR_PROFILE \
  --max-calls 20 --log-dir ./logs --out-dir ./artifacts

# Background comparison only inside needed description calls; main graph unchanged.
etl-parser run ./etl --descriptions --lambda-arn YOUR_FUNCTION --model YOUR_MODEL

# Clone-free source selection.
etl-parser run https://github.com/OWNER/REPO --ref COMMIT_SHA --path jobs \
  --log-dir ./logs --out-dir ./artifacts
```

Use `--ai-lineage improve` for broader review. Use `--no-background-comparison` to
suppress opportunistic shadow lineage. `--strict` exits nonzero for incomplete work;
without it the framework retains useful partial outputs. README and `run --help`
list the configurable budgets, provider options and source filters.

## Remaining limitations and optional future work

- Static analysis cannot prove arbitrary runtime Python, unknown UDFs, reflective
  imports, dynamic deployment values or every SQL/DataFrame/orchestrator API.
- Airflow is built in; other orchestrators require plugins. The extension architecture
  is available, not a claim that every orchestrator is already implemented.
- AI evidence validation and parser-assisted agreement are not independent truth.
  Exact-edge replacement is deliberately not automatic.
- Oversized contexts skip work; no automatic AI chunking, cross-run cache, source-first
  audit, raw-payload capture or concurrent AI scheduler is shipped.
- GitHub reads are serial and bounded; external LFS/submodules and Enterprise/App
  authentication are not implicitly fetched or configured.
- Log retention across runs belongs to the caller. A timeout cannot guarantee that
  an already-running remote Lambda stops or stops billing.
- Independent labeled accuracy evaluation and a live, explicitly configured AWS
  smoke test remain deployment validation tasks, not claims of this release.

## Where to look next

- [README and controls](../../README.md)
- [Original 11-step implementation review](../implementation-review.md)
- [Discussion/design record](../superpowers/specs/2026-09-17-ai-lineage-observability-github-design.md)
- [Follow-up implementation/delivery plan](../superpowers/plans/2026-09-17-ai-lineage-observability-github.md)
