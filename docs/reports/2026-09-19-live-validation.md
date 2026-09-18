# Live validation follow-up

## Configuration and scope

The user selected a replacement Anthropic-compatible endpoint and model.
Endpoint/model identifiers are omitted from this public report at the user's request;
examples use placeholders rather than either tested gateway. Tests use the existing private SDK
`AnthropicRunner`, with no custom headers, a 16,000-output-token cap,
300-second per-request timeout and 3,600-second AI-stage deadline.
The API key is entered through a hidden prompt, not stored in the repository,
configuration files or logs. No provider-specific transport was added.

The real corpus is `/Users/zidanesunesara/Desktop/Projects/de_agent/test_data/etl`,
with its matching sibling `catalog.json`. Filtering selects AI files while retaining
the full repository context and deterministic inventory: 28 jobs, 42 datasets,
35 table links, 177 column links and eight original unresolved diagnostics.

## Targeted real-file review

All seven target files eventually returned schema-valid, complete comparisons:
`dim_campaign.py`, `ingest_support_tickets.py`, `stage_customers.py`,
`stage_payments.py`, `stage_sessions.py`, `stage_shipments.py`, and
`stage_support_tickets.py`.

The first run made seven calls: six succeeded and `stage_customers.py` failed with
an SDK error without an HTTP status. The SDK implementation wraps HTTP transport
errors this way; the original run did not record the underlying cause type, so
the precise failure is unknown. One explicit retry succeeded. No automatic retry
was introduced. This successfully exercises the previously failing multi-proposal
validation path and replaces the earlier model-schema/timeout/skipped outcomes
with valid responses on this provider.

Both the initial run and retry kept all 177 column links and 35 table links unchanged.
The initial run generated 34 descriptions, inherited six and left two requested
descriptions unfilled. The separate customer retry generated four descriptions and
left four unfilled. Those six withheld descriptions had differing transformation
text: five quote-style differences and one quote/alias difference. The conservative
text comparison was not relaxed. These are separate run catalogs, not one merged
enriched catalog. Both corpus runs correctly retain `partial` status.

Reported usage: initial run 18,251 input / 44,346 output tokens; retry 3,551 input /
6,893 output tokens. Usage for the failed request is unknown. All reported cache
read/write counts were zero.

Artifacts: `/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-rn5r86ec/`.
Retry: `/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-2ai3_q2q/`.

## Fallback-only review

All four calls completed without request/schema failures, with descriptions disabled.
Only the four files selected by the deterministic diagnostics were called:
`derive_refund_fraud_flag.py`, `mart_funnel_conversion.py`,
`purge_pii_after_retention.py`, and `stage_marketing_attribution.py`.

Eight inferred column links and one table link were accepted: 185 column links and
36 table links in the effective graph. Every original column/table edge and all eight
original unresolved diagnostics were preserved (verified against the artifacts).
No descriptions were generated.

Four marketing-attribution column proposals were rejected because their evidence
quotes did not match the source. That file's comparison is therefore incomplete,
even though the model claimed completeness. The other three comparisons are complete.
The corpus run correctly remains `partial`; accepted additions are not independently
proven-correct mappings.

Reported usage: 13,174 input / 28,089 output tokens; zero reported cache reads/writes.

Artifacts: `/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-94rw91vv/`.

## Description plus background comparison: passed

One synthetic SQL file produced one valid response containing two descriptions,
two agreeing column mappings and one agreeing table dependency. AI lineage was
off: the main graph stayed exactly equal to the deterministic baseline. Comparison
and AI-lineage artifacts were separate. There were no warnings, no missing requested
descriptions and no extra audit call. Status was `success`.

SDK usage: 1,580 input and 1,824 output tokens; zero reported cache reads/writes.
Artifacts: `/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-0uu7r3qe/`.

## Code, documentation and regression checks

- README uses generic URL/model placeholders, with credentials supplied securely outside source.
- Separate [CLI](../cli.md) and [SDK/backend](../sdk.md) guides cover complete usage;
  README links to both. Tests check every registered CLI option, every analysis
  configuration field and scan option, local documentation links and Python syntax.
- A real-SDK/mock-HTTP regression verifies the exact endpoint prefix and model ID,
  token override, no custom routing headers, credential exclusion and client cleanup.
- SDK-wrapped HTTP transport errors now report whitelisted cause types, distinguishing
  timeouts from other transport failures without logging exception messages or bodies.
- Full suite: **226 passed**. Ruff, whitespace and offline source/wheel builds passed.

## Why the test appeared stuck

The manual checker prints only console errors and its final summary; detailed progress
is in `events.jsonl`. Requests were slow, not a persistent parser deadlock: the first
seven-file run measured roughly 80–187 seconds per call, the customer retry 148 seconds,
the fallback calls 67–163 seconds, and the small background-only call 41 seconds.
Calls within a run are serial. Two bounded test runs overlapped near the end.

All processes finished. Across the four test runs there were 13 requests: 12 returned
valid responses, and one failed before the successful explicit retry. No more live
requests are scheduled. The per-request limit was 300 seconds; successful API responses
still do not imply that every proposal or description passed acceptance checks.

## Interpretation

A validated AI response is not proof that every proposed mapping is semantically
correct. Acceptance remains evidence- and policy-controlled, original deterministic
edges/diagnostics are preserved, and missing descriptions or deferred changes remain
visible. Corpus runs can legitimately remain `partial` even after every model request
succeeds. These checks do not constitute a new full 28-file AI evaluation or prove
that the previous gateway's rate/quota limit has cleared.
