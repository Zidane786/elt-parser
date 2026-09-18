# Python SDK, larger response limits and failure handling

Follow-up: the user switched providers and the outstanding live request scenarios
were exercised successfully with the replacement provider, with partial-analysis limitations retained.
See the [live validation report](2026-09-19-live-validation.md). The gateway
blocker below describes the earlier run, not the current provider's state.

## What changed

- The framework works as a CLI and an importable Python library. `ParserClient.run`
  is synchronous; `await ParserClient.arun` supports async backends. Both use the
  same deterministic parser, AI policies and private SDK runners.
- Each client request has separate logs, metrics, results and a run ID. Optional
  artifacts and log files share that ID. `result.to_dict()` is JSON-compatible;
  raw source snapshots and runner credentials are excluded.
- Filesystem/GitHub scanning and artifact export do not run on the async event loop.
  Provider clients are closed by the framework unless supplied by the caller.
- Default output limit is **16,000 tokens** for analysis and legacy descriptions.
  CLI overrides: `run --max-output-tokens N`, `describe --max-tokens N`.
  Python overrides: `AnalysisConfig(max_output_tokens=N)` and
  `DescriptionEngine(..., max_tokens=N)`.
- Analysis request timeout is now **300 seconds**, with a **3,600-second** AI-stage
  deadline. Both remain configurable; the call limit remains 20. Larger limits
  reserve more budget and do not guarantee provider availability.
- Call-limit and deadline skips have distinct reason codes. Truncated model output
  is reported as `output_token_limit`. Schema failures retain safe field/error details;
  output instructions now explicitly require exact schema fields/enums and plain JSON.
- Provider failures record HTTP status and numeric Retry-After metadata, without
  dumping provider bodies. HTTP 401/403/404/429 stops later AI requests in that run.
  There are no implicit paid retries or automatic sleeps. Other file analysis and
  deterministic results remain available.
- README includes sync/async backend examples, optional AI configuration, result
  handling, concurrency/cancellation caveats and every updated CLI default.

## Validation

- Full automated suite: **221 passed** at the final release check.
- Private AI SDK blocked by import hook: **167 passed, four skipped**. This is a
  runtime-boundary test, not a newly provisioned dependency environment.
- Ruff and whitespace checks passed. Offline source and wheel builds passed.
- Built-wheel smoke test imported the public SDK from the wheel (not the working
  checkout), scanned the fixture repository (44 jobs) and serialized the result.
- Tests cover independent concurrent client requests, nonblocking scanning,
  log/artifact run identity, JSON serialization, configuration isolation, CLI token
  overrides, cancellation/failure finalization, provider circuit stopping, token
  truncation and safe schema-error handling. Prior AI policy tests still pass.
- Your real `/Users/zidanesunesara/Desktop/Projects/de_agent/test_data/etl` folder
  was scanned through the new Python client with its matching catalog: **28 jobs,
  42 datasets, 35 table links, 177 column links, eight unresolved diagnostics,
  zero AI calls**. Unresolved diagnostics are not silently erased.
- Fallback dry-run still selects the same four files: `derive_refund_fraud_flag.py`,
  `mart_funnel_conversion.py`, `purge_pii_after_retention.py`, and
  `stage_marketing_attribution.py`.

## Live gateway retest: blocked by provider

The seven previously failed/skipped files were selected from the full repository,
preserving import context. Live requests used `AnthropicRunner`, the supplied
gateway/credentials and selected model, **no custom headers**, 16,000 output tokens,
and the longer timeout/deadline. All seven returned provider errors before analysis.
That run predated the HTTP-status/circuit-stopping changes in this patch.

A single diagnostic request with safe HTTP-status logging confirmed **HTTP 429**.
This indicates gateway rate/quota rejection; the available evidence does not establish
the specific quota policy, its reset time, or any relationship to the larger token cap.
No further live requests were made after that confirmation. Main lineage retained
all 177 deterministic column links. No credentials or raw provider responses are committed.

Local temporary artifacts:

- Seven-file attempt: `/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-w5o_gyie/`
- HTTP-status diagnosis: `/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-45v8z8qr/`

The earlier multi-proposal bug is fixed and regression-tested. However, **successful
post-fix live validation of the seven files, the earlier model schema failure, and
the additional fallback/background-only live scenarios remain unverified** until
gateway availability is restored. Longer deadlines and clearer errors are not proof
that these external failures have disappeared. See the
[original live report](2026-09-18-gateway-validation.md) for the earlier successful
synthetic call and partial corpus evaluation.

## Backend deployment boundaries

The SDK is suitable for embedding in a service, but does not create an HTTP server,
authentication layer, distributed queue or cross-request rate limiter. Backend owners
must authorize source access, manage credentials and bound job concurrency. Cancellation
does not forcibly stop an in-flight worker thread or remote model billing. Review
partial results and unresolved diagnostics before publishing them as authoritative.
