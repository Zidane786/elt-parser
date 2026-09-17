# Implementation plan and delivery record: AI lineage, observability, GitHub

Created: 2026-09-17. Updated: 2026-09-18.
Status: requested core implementation and verification complete; release gates below.
Baseline: `a218033`, 105 tests.
Design: [discussion and design decisions](../specs/2026-09-17-ai-lineage-observability-github-design.md).
Delivery: [plain-language report](../../reports/2026-09-18-delivery-report.md).

This uses the repository's Superpowers plan convention. No installed Superpowers
execution skill was available; this is a reviewable engineering plan, not a claim
that an unavailable skill ran. The original implementation outline proposed more
modules and optional optimizations. This record names the actual delivered files
and separates future enhancements from the user's requested behavior.

## Controlling user decisions

- [x] Deterministic lineage and descriptions-off are the defaults; no AI calls.
- [x] Explicit `improve` reviews selected ETL files after deterministic parsing.
- [x] Explicit `fallback` reviews only files with diagnostics, partial lineage,
  or input/output jobs with no column edges.
- [x] Descriptions are independently opt-in.
- [x] When lineage AI is off, background comparison may **only reuse an already
  needed description call**. Never schedule another call just to audit.
- [x] Skip background review if descriptions already exist/inherit, or if combined
  source context will not fit. Never change main lineage in this mode.
- [x] Share lineage/description outputs in one structured response per eligible file.
- [x] Record ordinary parser work, AI decisions, changes, failures and metrics.
- [x] Console logs by default; optional log-folder persistence with generated names.
- [x] Use the private Agent SDK's Lambda Bedrock invoke runner, with configurable
  model, Lambda ARN/name, AWS profile, region and envelope.
- [x] Read GitHub repositories and subpaths through APIs without cloning.
- [x] Provide a simple report of requests, delivery, tests and remaining limits.

The user's isolated phrase “image generation” was interpreted in context as lineage
generation. Raster image generation is not part of this implementation.

## Task 1 — Mode and response contracts

- [x] `AnalysisConfig` validates stage controls, filters, budgets and SDK settings.
- [x] Strict versioned AI response/evidence models reuse native column identities.
- [x] AI provenance is assigned by application code and marked inferred, never exact.
- [x] All six lineage/description combinations have test coverage.
- [x] Defaults and dry-run need no SDK imports, credentials or calls.

Files: `etl_parser/ai_analysis.py`, `models.py`; `tests/test_analysis_modes.py`,
`tests/test_ai_lineage.py`.

## Task 2 — Structured logging

- [x] Isolated stderr logger; optional private JSONL files; no root-handler changes.
- [x] Run/event/span/request IDs, actors, reasons and bounded metadata.
- [x] Unique run directories, rotation limits, atomic metrics/manifests.
- [x] No source/prompt/response/exception-message capture; best-effort credential redaction.
- [x] Thread, cancellation, disk, rotation, delayed-close and stdout-isolation tests.
- [x] Audit failures stop subsequent AI work and roll back file-level AI mutations.

Files: `observability.py`, instrumented CLI/scanner/pipeline/description engine;
`tests/test_observability.py`.

## Task 3 — Metrics

- [x] Source/read/parse/import/ZIP counts, findings, final graph counts and diagnostics.
- [x] Stage/API/model durations, bounded latency samples, whole-run throughput.
- [x] SDK usage availability and token counts, request outcomes, policy changes,
  description outcomes, table/column comparison counts.
- [x] Cost and independent accuracy remain unknown rather than invented.
- [x] Performance and run IDs remain outside deterministic lineage content.

Metrics are bounded operational observations, not complete distributed tracing or
independent semantic correctness measurements. See the report for definitions.

## Task 4 — Provider-neutral source access

- [x] `SourceProvider`, `LocalSource`, `GitHubSource` feed the same `ScanIndex`.
- [x] Preserve file/directory entry scope and sibling helper resolution.
- [x] Shared module indexing and ZIP inspection without extraction or execution.
- [x] Preserve existing parser/orchestrator extension contracts and golden outputs.

Files: `sources.py`, `scanner/repo.py`, `pipeline.py`.

## Task 5 — Clone-free GitHub

- [x] Allowlisted repository URL, separate ref/subpath, immutable commit/tree/blob reads.
- [x] Recursive tree listing with nonrecursive recovery when truncated.
- [x] Bounded bytes/files, request timeout/retries and rate-limit waits.
- [x] Environment token support and no redirects to other hosts.
- [x] Explicit diagnostics for unavailable, oversized, binary, LFS and unsafe entries.
- [x] Local/remote helper and ZIP parity; no source worktree writes.
- [x] HTTP 401/403/404/429/5xx, >1,000 entries, truncation and archive-safety tests.
- [x] Live read-only scan of this repository at `a218033` matched all local fixture
  lineage: 28 jobs and 177 column edges, equal native documents at the same revision.

Files: `sources.py`, `tests/test_github_sources.py`.

## Task 6 — File eligibility and context

- [x] Deterministic diagnostic/partial/sparse reason codes; no model-owned scope changes.
- [x] Include/exclude globs, source digests and numbered file evidence.
- [x] Bounded context and dry-run work plan.
- [x] Only the triggering file's source is sent; no autonomous repository traversal.

Helper sources are used by deterministic analysis. AI gets that result and the
selected file, not a separate helper-source expansion or a whole-repository prompt.
Oversized files are skipped; automatic paid chunking is intentionally not included.

## Task 7 — Shared SDK calls

- [x] SDK-only transport, explicit model/profile/ARN/region/envelope controls.
- [x] One attempted call per selected file; no implicit paid repair/retry calls.
- [x] Call/context/output-token/total-token/deadline/timeout controls.
- [x] Preserve descriptions and inherit exact identity descriptions before calls.
- [x] Same-call background comparisons; oversized-source fallback stays one call.
- [x] Usage excludes raw provider metadata; unavailable usage is reported separately.
- [x] Fake-SDK and real-SDK/mock-Lambda contract tests; no live AWS charges.

The legacy saved-lineage `describe` command remains evidence-only: it lacks source
snapshots. Use `run SOURCE --descriptions` for source-backed combined analysis.

## Task 8 — Validation and comparison

- [x] Validate local JSON schema, file/job ownership, source digest, lines, exact quote,
  physical IDs and supplied schema membership.
- [x] Reject tool requests, bad/truncated responses, fabricated references and unknown fields.
- [x] Separate column/table agreements and differences; report textual transformation differences.
- [x] Record incomplete and assisted reviews; missing proposals are not proven parser errors.
- [x] Adversarial, malformed-response, timeout and schema tests.

All response fields must pass structural validation; a structurally invalid response
is rejected as a unit. Validly structured lineage proposals are evidence-checked
individually. Evidence presence does not establish semantic truth.

## Task 9 — Merge policy and artifacts

- [x] Preserve deterministic snapshot, exact mappings and original diagnostics.
- [x] Off mode cannot mutate main lineage; proposals stay in separate files.
- [x] Enabled modes add validated gaps; conflicting exact mappings remain deferred.
- [x] Record changes, request IDs and inferred model provenance.
- [x] Publish only requested missing descriptions compatible with supported mappings.
- [x] Roll back file-level mutations on failure/audit loss.
- [x] Unique private artifact folders, content hashes, source digests and atomic manifests.

Files: `ai_analysis.py`, `artifacts.py`. No destructive replacement/approval workflow
is shipped; this is an intentionally additive policy.

## Task 10 — CLI and Python controls

- [x] Unified `run`, JSON config, CLI/env overrides, budgets and filtering.
- [x] Native/catalog/artifact output, logs, strict exit behavior and dry-run.
- [x] Local and GitHub inputs; schema/bindings/products and trusted plugins.
- [x] Retain `scan`, `describe`, `export`, `impact`, `products`.
- [x] Synchronous and asynchronous Python analysis APIs.

See `etl-parser run --help` and README examples for shipped flags; proposed flags
in the design notes are not an alternate CLI reference.

## Task 11 — Verification

- [x] Original 105-test baseline retained and expanded with policy/source/logging tests.
- [x] Rechecked `/Users/zidanesunesara/Desktop/Projects/de_agent/test_data/etl`
  against its matching catalog: all 28 golden script records equal.
- [x] Full private-SDK suite and isolated no-private-SDK suite.
- [x] Repeatable 500-file synthetic benchmark, three runs per logging mode; equal graphs.
- [x] Live GitHub read-only parity check; no live model, Lambda or Glue invocation.
- [x] Ruff, packaging and whitespace checks.

Exact counts/results are recorded in the delivery report. Confidence and AI agreement
are not measured accuracy. No independent production holdout accuracy is claimed.

## Task 12 — Documentation and release

- [x] README with actual controls, examples, privacy and source limits.
- [x] Preserve discussion in design notes and reconcile actual implementation choices.
- [x] Plain-language request-to-delivery report with support and limitations.
- [x] Prepare verified changes for the authorized commit/push. The final remote result
  and commit hash are recorded in the user-facing handoff after the operation, not
  asserted by this pre-commit document.

## Optional future enhancements — not shipped or implied by this release

These were engineering possibilities in the initial outline, not prerequisites for
the requested default-off, same-call, logged workflow:

- Cross-run AI/blob caches; concurrent AI scheduling; model-specific dollar pricing.
- Source-first independent AI evaluation and independently annotated accuracy holdouts.
- Automatic source chunking/helper snippet expansion; configurable coverage thresholds.
- Human-reviewed destructive graph replacements and stale-approval handling.
- GitHub Enterprise/App authentication and implicit external-submodule/LFS retrieval.
- Raw AI payload capture (deliberately absent); logging retention across runs is caller-owned.
- Arbitrary runtime/UDF semantics, new orchestrator plugins and runtime OpenLineage ingestion.

Future work must preserve the invariant: lineage-off plus descriptions-on never
schedules an additional model call for background comparison.
