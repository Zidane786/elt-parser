# Optional AI lineage, observability, and clone-free GitHub scanning

Status: discussion/design record, reconciled with the implemented core on 2026-09-18.
Created: 2026-09-17.
Companion plan: [implementation tasks](../plans/2026-09-17-ai-lineage-observability-github.md).

### Implementation reconciliation (read before the original design notes)

The requested AI modes, same-call background comparison, logging, metrics, SDK
controls and clone-free GitHub source are now implemented. The
[delivery report](../../reports/2026-09-18-delivery-report.md) and README describe
the shipped contract. Sections below preserve the broader design discussion;
proposed options there are **not** a list of shipped CLI flags.

Actual choices and deliberate limits:

- Implementation is in `ai_analysis.py`, `observability.py`, `sources.py` and
  `artifacts.py`, rather than the more fragmented package layout initially outlined.
- Fallback uses existing diagnostic categories, non-exact edges and input/output
  jobs with no column edges. Filters and budgets are configurable; arbitrary
  numerical coverage thresholds and helper-context expansion are not shipped.
- One attempted SDK call per selected file; no AI retries, automatic chunking,
  cross-run cache, concurrent scheduler or separately paid background audits.
- SDK text output is validated with strict local models. Structurally malformed
  responses are rejected as a unit, then valid proposals are evidence-checked
  individually. All comparisons are parser-assisted, not source-first evaluations.
- Merge is additive: exact mappings and original diagnostics remain. AI proposals
  carry model/request/evidence provenance. No destructive replacement approvals.
- Legacy `describe` is saved-evidence-only. `run SOURCE --descriptions` supplies
  source-backed combined analysis; there is no legacy `describe --source` flag.
- Log folders contain `events*.jsonl`, `metrics.json`, `manifest.json`; output
  folders contain `decisions.json`, `changes.json`, work plan, graphs and catalog.
  Events exclude raw payloads and exception messages/stacks. Raw payload capture
  is deliberately not available. Graph/change artifacts do contain expressions.
- GitHub requests are serial, bounded and commit-pinned. Tokens use the environment
  or Python constructor; Enterprise/App flows and disk caches are not shipped.
- Operational counts, timings, tokens and differences are measured. Monetary cost,
  independent accuracy, global coverage ratios and production capacity are not inferred.

Live GitHub/local fixture parity and a repeatable 500-file local benchmark have
been run. Production AWS/model behavior and independent holdout accuracy remain
unverified, explicitly separate from passing SDK mock contracts.

### Controlling clarification from the user

Descriptions remain off by default. With AI lineage off, a background AI lineage
comparison is allowed **only as a by-product of an already-needed description call**.
If lineage needs a separate call, skip it. Do not schedule a call just to audit,
including when all descriptions already exist or are inherited. The main lineage
remains unchanged. This clarification supersedes any earlier standalone-audit
proposal below. Explicit fallback/improve modes still authorize their own AI work.

## 1. Intent and interpretation

Keep the deterministic ETL parser as the default and add optional AI assistance,
auditable decision-making, operational metrics, and a remote repository source.
The user said "image generation" once but described lineage generation throughout;
this design interprets that as AI-assisted lineage, not raster image generation.
Actual image generation is outside this proposal unless separately confirmed.

This document records requirements, proposed defaults, and engineering decisions.
It does not assert that all proposed CLI options or modules already exist.

## 2. Requirements captured from the discussion

1. Deterministic lineage remains the default; AI lineage is off by default.
2. An explicit improvement mode runs the parser first, then asks AI to review and
   improve supported lineage, retaining a record of every proposed/applied change.
3. An independently enabled fallback mode calls AI only for particular files with
   failures, uncertain results, or evidence of unexpectedly sparse extraction.
4. Record control transfers, decisions, reasons, findings, failures, and provider
   interactions for both ordinary deterministic and AI-assisted runs.
5. Show logs on the console by default; optionally persist detailed logs in a
   caller-selected directory, using framework-generated filenames.
6. Produce useful correctness/coverage indicators and performance/usage metrics.
7. Allow independent lineage and description generation. When both are requested,
   share source context and a structured response rather than calling twice for
   the same work unnecessarily.
8. Optionally generate AI lineage alongside descriptions into a separate artifact
   and compare it with deterministic lineage without changing the main graph.
9. Enforce our output formats, validate AI proposals, and preserve provenance.
10. Use the existing private Agent SDK Lambda Bedrock invoke runner. Expose model
    ID, Lambda ARN, AWS region/profile, and the existing envelope-mode setting.
11. Accept GitHub repositories/files/directories through read/list APIs, without
    cloning or checking out the repository locally. Reuse local parsing semantics,
    helper resolution, ZIP analysis, and exports as far as remote access permits.
12. Make behavior configurable and document which controls are available versus planned.

## 3. Current implementation versus new work

Already implemented:

- SQLGlot and Python AST/frame lineage, source-located diagnostics, graph building,
  Airflow/product relationships, local repository/file/directory/ZIP scans.
- Optional description engine with SDK `BedrockInvokeLambdaRunner`.
- Lambda ARN, model, region, AWS profile, Web Adapter toggle, output-token limit.
- Existing-description preservation, identity-description inheritance, malformed
  response handling, exact-lineage eligibility, synchronous and asynchronous APIs.

New work:

- AI lineage proposals, file eligibility decisions, validation, controlled merging,
  separate AI snapshots, comparison reports, and combined responses.
- Structured run-wide logging, decision/change ledgers, metrics, persistence and budgets.
- Source-provider abstraction and clone-free GitHub reader.
- Unified run configuration, controls, documentation, and adversarial/parity tests.

The original rule "no LLM outside describe/" is superseded for explicitly enabled
`ai_analysis.py` stages. Both description paths use the same private SDK transport;
default deterministic scanning still makes no model calls.

## 4. Explicit mode matrix

Two independent dimensions: `ai_lineage` and `descriptions`. Background comparison
is opportunistic within description calls, never a separately scheduled AI stage
while AI lineage is off. A disable switch may suppress even this by-product.

| AI lineage mode | Description generation | Effect |
| --- | --- | --- |
| `off` (default) | off (default) | Deterministic lineage only; no LLM initialization/calls |
| `off` | on | Descriptions from existing evidence; main lineage unchanged |
| `off` | on, combined response feasible | Background AI lineage and comparison from the same description call; main lineage unchanged |
| `fallback` | off/on | AI only for eligible files; accepted gap-filling may enter main lineage |
| `improve` | off/on | AI reviews selected ETL files after parsing; accepted gap-filling may enter main lineage |

`fallback` and `improve` are mutually exclusive enum values. Background comparison
never authorizes an extra AI call or main-lineage mutation. Descriptions default
off and never implicitly enable fallback/improve. If no description call is needed,
the background comparison is skipped with a reason.

Always retain `lineage.deterministic.json` when any AI lineage work is requested.
`lineage.json` is the effective graph: the deterministic result with any explicitly
permitted and validated AI additions. In off/audit mode these graphs must be equal.
Description enrichment never modifies a lineage graph.

## 5. Control flow and ownership

Sequence:

`source snapshot -> deterministic parsers -> eligibility policy -> optional AI
analysis -> schema/evidence validator -> comparison -> merge policy -> exports`

The orchestrator is ordinary application code, not an autonomous model deciding
what it may read or overwrite. It owns budgets, scopes, retries, and acceptance.

- Source provider decides neither lineage nor AI eligibility.
- Parser emits findings and diagnostics, not model calls.
- Eligibility policy returns typed reason codes and measured signals per file.
- AI proposes bounded records and short evidence-based explanations.
- Validator checks contracts and references; passing validation does not prove truth.
- Merge policy determines application versus rejection/deferment.
- Every transition records actor, stage, parent event, reason, and affected IDs.

Log observable decisions and concise evidence summaries, not private chain-of-thought.
The model cannot authorize new tools, expanded scope, spending, or graph replacement.

## 6. Fallback eligibility

Evaluate after deterministic parsing, once per entry file (including all jobs/tasks
originating from that file). Possible reason codes:

- `parse_failed`: parser failure or unsupported syntax in the selected file.
- `partial_lineage`: one or more partial edges/unknown-column diagnostics.
- `unresolved_reference`: table/path/import expression remains unresolved.
- `sparse_extraction`: detected I/O operations but no corresponding datasets/edges,
  or known output schema with unexpectedly few mapped output columns.

An empty helper, constants module, input-only job, DDL-only file, or intentionally
column-free operation is not automatically a failure. A missing denominator is
`unknown`, not 0% coverage. Never infer confidence merely from a high edge count.
Thresholds, trigger categories, inclusion/exclusion filters, and per-file limits
are configurable and appear in the run manifest.

Fallback is scoped to the triggering file. Directly imported helper/ZIP snippets
may be read as bounded supporting evidence when enabled; no whole-repo AI sweep.
Supporting evidence and any inability to resolve it are logged. The model response
may only propose changes for authorized jobs belonging to the triggering file.

## 7. AI contracts, evidence, and merging

Introduce versioned Pydantic models with extra fields forbidden:

- `AIAnalysisRequest`: source identity/revision/digest, authorized file/jobs,
  numbered source snippets, bounded helper context, schema/bindings, requested
  outputs, deterministic evidence when applicable, prompt/schema version.
- `AIAnalysisResponse`: proposed datasets/jobs/table and column edges,
  unresolved items, optional description candidates, evidence references, and
  short rationale strings. Avoid model-created tool instructions.
- `EvidenceRef`: known source ID, digest, line span, and verifiable text reference.
- `DecisionRecord`, `ChangeRecord`, `ComparisonReport`, and `RunMetrics`.

Use SDK structured-output support where compatible, plus local validation in all
cases. Share native model fields and canonical identity helpers with existing
`LineageDocument`/`WorkerResult`; do not maintain a loosely related AI graph format.
Do not let the model assign trusted parser provenance or exact confidence.

Validate syntax/schema, reference existence, canonical IDs, source hashes/spans,
job/file ownership, known schema compatibility, duplicate records, fabricated
references, and output size. Ambiguous environment names remain unresolved unless
bindings/evidence actually identify them. Detect truncated/blocked/tool responses.
Code comments, strings, and repository documentation are untrusted prompt data.
AI cannot execute scanned code or request arbitrary filesystem/network access.

Default merge policy when AI lineage is enabled: `fill-gaps`.

- Preserve deterministic exact edges and all original evidence.
- Retain partial deterministic edges; add accepted candidate edges with AI origin
  and resolution links. Do not silently erase the diagnostic being investigated.
- Label AI-origin edges `inferred`/`partial` with model/request/evidence references,
  never `exact` based on the model's self-assessment.
- Conflicting or replacement proposals remain deferred in the change ledger.
- A future/explicit reviewed-patch application may replace/remove an edge only
  with matching baseline digest, recorded reviewer/action, and compatible source
  snapshot. Reject stale approvals. No automatic deletion of exact edges.
- Persist accepted, rejected, unchanged, and deferred decisions with before/after,
  evidence, validation results, policy version, and model/request metadata.

Failures or exhausted budgets leave the deterministic result usable and the AI
stage visibly incomplete. Never report an empty AI response as successful repair.

## 8. Description generation and shared AI work

Current engine behavior:

1. Read saved lineage and catalog; work on a copy of the catalog.
2. Visit columns in dependency order, condensing cycles.
3. Preserve existing descriptions. Inherit a known upstream description for an
   exact identity mapping without calling the model.
4. Skip partial lineage; for other eligible columns, send recorded transformation
   expressions, references/schema, and upstream/table descriptions to the SDK.
5. Accept a non-empty description from a valid, non-blocked response; otherwise warn.

Current prompts contain extracted evidence, not the full original source. They
cannot independently reconstruct or verify all missing lineage. The new source
context loader is required for the requested additional review.

Proposed combined analysis:

- Batch by file/job and bounded source chunk, not one independent call per column.
- If lineage and descriptions/audit are requested for the same context, request
  both fields in one structured response and store them separately.
- Preserve descriptions and identity inheritance before scheduling model work.
- Deduplicate work by snapshot, context digest, schema/bindings, requested outputs,
  prompt version, model/settings, and privacy scope. Do not reuse responses across
  changed sources or authorization scopes.
- Validate description and lineage sections independently. A rejected lineage
  patch must not make a conflicting description appear grounded in main lineage.
- Only publish a description if its dependencies are compatible with the selected
  graph; store rejected/conflicting candidates separately with reasons.
- A pre-existing-lineage `describe` command without source context can continue
  evidence-only descriptions, but must refuse source-backed audit rather than
  inventing missing code. Accept `--source`/a verified accessible source manifest.
- If all descriptions already exist or are inherited, make no extra call for
  background comparison. Missing context or incompatible response/token limits
  similarly skip comparison rather than incur an extra request.

"One call" means one response per bounded work unit where possible, not a guarantee
of one request for an entire repository. Large files/context limits and explicit
retries may require multiple requests, all measured. Log cache reuse distinctly.

## 9. Shadow lineage and comparison

Write AI-only candidate evidence to `lineage.ai.json`, with scope/completeness in
the manifest. It is not a replacement for the deterministic snapshot. With audit
enabled, compare canonical graph content and emit `lineage.comparison.json`:

- agreement, AI-only, deterministic-only, and conflicting source/target edges;
- direct versus indirect roles, transformation/source differences, and unresolved
  versus concrete references;
- excluded/unreviewed files, failed chunks, and incompletely reviewed scopes.

Canonicalize IDs and compare semantic edge signatures, excluding timestamps,
provenance-only differences, and harmless source ordering. Report transformation
text differences without assuming string differences prove semantic inequality.
An edge absent from incomplete AI output is not evidence that the edge is false.

Record review style: `assisted` includes parser results, whereas `source-first`
audit withholds those results from the model where practical. Sharing a
description call can introduce anchoring/correlation; report that limitation.
Agreement/disagreement is not measured precision, recall, or correctness. Those
metrics require an independently annotated reference corpus/human review.

## 10. Logs, artifacts, privacy, and metrics

Console logging is enabled by default at INFO on stderr. Keep stdout machine-readable.
Use standard Python logging with context fields; library code must not globally
replace handlers. Logging is independent of AI and covers deterministic-only runs.

`--log-dir /chosen/folder` creates an exclusive run directory such as
`20260917T120000Z_<run-id>/` with `events.jsonl`, `decisions.jsonl`,
`changes.jsonl`, and `metrics.json`. `--out-dir` controls graph/catalog artifacts;
it is distinct from log location. Audit artifacts are produced even without a log
directory. No output path is auto-committed/uploaded; no silent overwrite on collision.

Events include UTC timestamp, monotonic duration, run/event/parent IDs, actor,
stage, source/revision/hash, file/job/parser IDs, action/status/reason, resource
counts, retries, and sanitized exception class/stack at DEBUG.

At INFO show progress, control transfers, warnings, summaries, and provider calls.
At DEBUG record individual findings/decision metadata and import/ZIP resolution.
File event logs may use DEBUG independently of console verbosity. Summaries must
include skipped/dropped events if sampling/queue pressure occurs; never sample
away errors, decisions, or applied changes. Bounded queues/backpressure and
streaming writes prevent logging from exhausting memory.

Never log AWS/GitHub credentials, authorization headers, session tokens, or raw
secrets. Source text, prompts, and raw responses are excluded from ordinary logs
by default. `--capture-ai-payloads` is a separate explicit opt-in for restricted,
redacted artifacts; redaction is best-effort, not a confidentiality guarantee.
Log source/prompt/response digests and bounded sanitized validation details by
default. Keep secure file permissions where supported, rotation/size/retention
controls, and tests with planted secrets. No telemetry leaves the machine by default.

Metrics to collect (with units and definitions):

- Source inventory: entries listed, eligible/read/parsed/skipped/failed files,
  reasons, bytes, ZIP members/rejections, helper-resolution outcomes.
- Performance: total and per-stage durations, per-file/parser durations, throughput,
  provider/API latency distributions and sample counts, queue depth/concurrency,
  retry/rate-limit wait time, cache hits/misses. Optional portable memory metrics;
  mark unsupported measurements unavailable and measure instrumentation overhead.
- Lineage: jobs/datasets/table/column edges, confidence buckets, diagnostic kinds,
  output columns mapped versus known schema columns where a denominator exists.
- AI: eligible/selected/completed/skipped files, trigger reasons, request attempts,
  provider-reported tokens/cache usage, response validation failures, accepted/
  rejected/deferred changes, budgets remaining, call savings through reuse.
- Descriptions: existing/inherited/generated/skipped/failed counts and provenance.
- Comparison: agreements/differences and reviewed/unreviewed scope; not accuracy.
- Reliability: status per stage, partial-run causes, cancellation, export/log errors.

Report provider cost only when configured pricing/usage supports an estimate;
otherwise `unknown`, not zero. Do not label model self-confidence as calibration.
Keep per-file detail in logs/artifacts, not unbounded metric label cardinality.

The run manifest records effective redacted config, source snapshot, tool/SDK/
parser/prompt versions, enabled stages, output hashes, completeness, and status.
Nondeterministic timing/run IDs belong in sidecars, not deterministic graph content.

## 11. GitHub source provider (no clone)

Introduce a read-only `SourceProvider` boundary: resolve snapshot, list entries,
read bounded bytes/text by source identity, and report capabilities/diagnostics.
Implement local and GitHub providers behind the same `ScanIndex`/`SourceFile`
contract. Remove `Path.exists/is_file/relative_to` assumptions from orchestration.

- Accept local paths or `https://github.com/OWNER/REPO[.git]`, with separate
  `--ref` and `--path` options. Default to the repository default branch, resolve
  once to a commit SHA, and pin every read to that immutable snapshot.
- Inventory the Git tree. If a recursive listing is truncated, walk nonrecursive
  subtrees; never silently claim complete coverage of a truncated listing.
- Read eligible blobs through authenticated APIs; retain repository, commit,
  logical path, blob/content digest, and source spans as provenance.
- Do not use `git clone`, git checkout, tarball checkout, or reconstruct a local
  worktree. Default to bounded in-memory content; optional content-addressed disk
  cache is explicitly enabled and disclosed, not a hidden checkout.
- Classify every listed entry; parse supported files and report skips. "Every
  file" does not mean execute/read unbounded binaries, secrets, or unsupported formats.
- Preserve module-relative paths, bounded imports and local ZIP helper behavior.
  Fetch ZIP bytes subject to transport/decompression/member limits, inspect
  without extraction, and use `archive.zip!/member.py` logical identities.
- Detect symlinks, submodules, LFS pointers, unavailable/oversized/binary files,
  permission failures and missing snapshots. Do not follow external repositories
  or LFS downloads implicitly; report incomplete coverage and suggested actions.
- Handle rate limits, Retry-After/reset information, retryable network failures,
  bounded exponential backoff/jitter, per-request timeouts, cancellation and caches.
- Support read-only GitHub tokens through environment/credential callbacks and
  future GitHub App tokens. Do not expose tokens in CLI arguments or logs.
- Restrict host/API URLs, redirects and path traversal; never forward credentials
  to an untrusted redirect. Enterprise API roots require explicit configuration.

Provide reusable read/list methods, not an LLM-controlled general network tool.
The orchestrator assembles authorized source context through those methods.
Private GitHub access does not itself authorize sending source to an LLM; an AI
opt-in and source-scope policy are still required, with visible disclosure.

GitHub cannot guarantee unrestricted filesystem equivalence. Equivalent snapshots
within supported limits should yield the same semantic lineage; availability,
API limits, permissions and external dependencies must be represented honestly.

Verified API references (2026-09-17):

- Git Trees supports recursive inventory but can truncate; use subtree traversal
  when needed. [GitHub Git Trees documentation](https://docs.github.com/en/rest/git/trees).
- Contents supports file/directory reads but has directory/file-size constraints
  and special symlink/submodule behavior. [GitHub Contents documentation](https://docs.github.com/en/rest/repos/contents).
- Authentication and rate limits affect throughput; honor provider limits rather
  than assuming local-filesystem latency. [GitHub rate-limit documentation](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api).

## 12. Proposed controls and defaults

These are design targets, not currently shipped flags. Retain existing commands;
add a unified `run` command/config model without breaking `scan/export/describe`.

| Group | Proposed controls |
| --- | --- |
| Stages | `--ai-lineage off\|fallback\|improve` (off), `--descriptions` (false), optional disable switch for opportunistic combined-call comparison |
| Scope | source path/URL, `--ref`, `--path`, include/exclude globs, job/dataset/column filters |
| Fallback | trigger categories, minimum mapped-column ratio when known, max files, helper-context depth |
| Merge | `fill-gaps` default; explicit reviewed-patch application for replacements |
| Provider | `--lambda-arn`, `--model`, `--region`, `--aws-profile`, `--web-adapter/--no-web-adapter` |
| Budgets | max calls, input/context/output tokens, total token budget, deadline, concurrency, retries |
| Descriptions | missing-only default; preserve human/verified text; explicit overwrite-AI policy |
| Observation | console/file level, `--log-dir`, `--out-dir`, metrics path, payload-capture opt-in |
| Source limits | max files/bytes/ZIP sizes/depth, GitHub request concurrency/retries, optional cache |
| Run policy | `--dry-run`, strictness/error categories, AI failure behavior, deterministic fallback |

Use one validated config across CLI/Python APIs. Precedence: CLI > environment >
config file > defaults; output the redacted effective config. Reject contradictory
options rather than guessing. Dry-run may perform deterministic scanning/read-only
source fetches, but makes zero LLM calls and reports proposed eligibility/work;
token/cost estimates are approximate or unknown.

Use the SDK's existing normal credential chain when no AWS profile is provided.
Profile selection is not automatic SSO/login; expired/missing credentials produce
actionable errors. Never duplicate Bedrock transport outside the SDK. Support
both existing Lambda envelopes and explicit model IDs without a hard-coded model.

## 13. Acceptance boundaries

- Off/off/no-audit makes zero SDK imports/provider calls and preserves baseline output.
- Audit-only never changes effective lineage, including on provider/schema failures.
- Fallback touches only eligible files and permitted supporting snippets.
- Every AI-applied change has validated evidence, provenance, and a decision record.
- Combined mode reuses work; description-only still works against saved lineage.
- Local/GitHub semantic parity is demonstrated against the same pinned fixture snapshot.
- Logging never corrupts stdout JSON or exposes planted credentials by default.
- Partial scans, failed chunks, unknown metrics, and exhausted budgets remain visible.
- No source-code execution, clone, automatic external dependency fetch, or hidden upload.
- Independent annotated holdout tests, not AI agreement, establish future accuracy claims.

## 14. Decisions awaiting deployment details, not blocking design

- Production Lambda/model/region/profile and GitHub authentication are caller settings.
- Set initial numerical budgets/thresholds using benchmark results; default concurrency
  starts at one and retries are bounded. Do not invent a dollar price/model guarantee.
- Confirm only if actual raster images were intended; they are not part of lineage AI.
- Exact SDK structured-output compatibility must be exercised against the chosen model;
  local validation remains mandatory regardless of provider enforcement.
