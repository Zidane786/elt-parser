# Runner options, usage documentation and live gateway validation

Date: 18 September 2026. Extends the release at `81f569d`.
Status: implemented and locally tested; release checks passed. Live corpus
evaluation remains partial, with follow-up live checks still outstanding.

Follow-up: [SDK, token defaults and reliability report](2026-09-18-sdk-and-reliability.md)
records the new public client, larger limits and the subsequent HTTP 429 live-test blocker.

## Requested changes delivered locally

- `--runner lambda-bedrock-invoke` and `--runner lbi` select the same private SDK
  `BedrockInvokeLambdaRunner`; serialized settings use the full name.
- `--runner anthropic` selects the private SDK `AnthropicRunner`.
- `bedrock` is intentionally rejected to avoid confusion with the SDK's separate
  direct Bedrock runner, which is not wired into this framework.
- Anthropic settings: HTTPS `--base-url`, `--api-key`/`ANTHROPIC_API_KEY`, and optional
  `--extra-headers` JSON or `--extra-headers-file`. No custom headers by default.
- Both `run` and legacy `describe` support runner selection. Defaults/dry-run/help
  require no provider initialization. Only the selected runner is constructed.
- Factory-owned HTTP clients are closed; caller-injected SDK runners remain caller-owned.
- Keys/header values are excluded from config serialization and ordinary logging.
  HTTPS/URL/header validation rejects unsafe credential placement and header injection.
- README now covers every command/registered option, feature support, input/output
  formats, examples, environment precedence, safety limits, caching and live checks.
- `--help` works for the root, groups and every leaf command, without credentials.
- `catalog.template.json` is a validated example of the catalog used for `--schema`
  and `--prior`; README explains their different purposes.

For this deployment, streaming and buffered Lambda functions have different ARNs.
The framework currently uses **the non-streaming/buffered ARN**, `complete()`, buffered
`invoke`, and `/bedrock` with the Web Adapter. It does not use the streaming ARN,
`invoke_with_response_stream`, or `/bedrock_stream`.

## Actual live gateway used

- SDK runner: `AnthropicRunner`, not a fake runner or direct custom HTTP client.
- Base URL: `https://aigateway-beta-api.godigitaltc.com/aigw`.
- Model: `codex/gpt-5.6-terra`, as explicitly requested by the user.
- Authentication: the user-supplied API key, entered through a hidden terminal prompt.
- Extra headers: **none**, following the user's correction.
- Raw keys were not written to source/config/log files or committed.

Selecting `anthropic` means the gateway speaks the Anthropic Messages API shape;
the model ID can identify a different underlying model served by that gateway.
An actual Claude-model live test was not performed in this pass.

## Live test 1 — synthetic ETL: passed

One request analyzed a synthetic SQL CTAS with identity and computed output columns:

- Valid structured response, no warnings.
- Two column mappings agreed with deterministic parsing; main graph unchanged.
- Two descriptions generated in that same request.
- One table dependency agreed.
- SDK usage: 1,536 input tokens, 576 output tokens, zero reported cache reads/writes.

Artifacts:
`/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-yfrma9om/`

## Live test 2 — requested real ETL folder: partial

Source: `/Users/zidanesunesara/Desktop/Projects/de_agent/test_data/etl`.
Schema: its matching sibling `/Users/zidanesunesara/Desktop/Projects/de_agent/test_data/catalog.json`.
Mode: `improve` plus descriptions, 28-call cap, 8,192 output tokens/request,
120-second request timeout, 1,200-second AI-stage deadline, no implicit retries.

| Observation | Result |
| --- | ---: |
| Deterministically scanned jobs | 28 |
| Attempted live requests | 25 |
| Returned model responses | 23 |
| Files with validated comparisons | 21 |
| Failed file analyses | 4 |
| Files skipped at deadline | 3 |
| Baseline column links | 177 |
| Accepted inferred column additions | 12 |
| Effective column links | 189 |
| Table links | 35, unchanged |
| Original diagnostics | 8, retained |
| Generated descriptions | 56 |
| Inherited descriptions | 14 |
| Rejected description proposals | 68 |
| Requested descriptions left unfilled | 62 |
| Deferred lineage changes | 33 |
| Rejected lineage proposals | 18 |
| SDK-reported input tokens | 78,911 |
| SDK-reported output tokens | 52,438 |
| SDK-reported cache read/write tokens | 0 / 0 |

Every original deterministic column edge is still in the effective document.
The baseline is preserved separately. Inferred additions are not proven-correct
repairs, and original diagnostics are not silently removed.

Mean request duration was approximately 48.0 seconds; measured recent p95 was
88.5 seconds over 25 request-duration samples (including failures). Two requests
timed out at approximately 120 seconds. Usage for timed-out requests is unknown;
the accounting budget retains conservative reservations. Dollar cost is unknown.

### Failures and skipped files

| File | Outcome |
| --- | --- |
| `dim_campaign.py` | Structured-response validation failure; rejected |
| `ingest_support_tickets.py` | Application validation `TypeError`; root cause fixed locally afterward |
| `stage_customers.py` | Request timeout; no automatic paid retry |
| `stage_payments.py` | Request timeout; no automatic paid retry |
| `stage_sessions.py` | Not called because the AI-stage deadline expired |
| `stage_shipments.py` | Not called because the AI-stage deadline expired |
| `stage_support_tickets.py` | Not called because the AI-stage deadline expired |

The application bug reused the name `known` for both the allowed-dataset set and
a per-dataset column schema. An unavailable column schema could replace the allowed
set with `None`, breaking validation of later proposals. Separate variables now
preserve both scopes. A regression test covers multiple column and table proposals
where the source schema is unknown. Other files could also have experienced overly
strict rejection from the old variable reuse; this run is not a clean post-fix evaluation.

Schema-failure logging now includes bounded error locations/types and response
digests, without raw model responses. The original in-flight run used the code
loaded before these fixes; the fixed code has **not** been live-retested yet.

Artifacts:
`/var/folders/k5/snqv85551yl391q5tm350mwr0000gr/T/etl-parser-gateway-zu86mt5o/`

Within that folder, `artifacts/run_10fadbd15e4345c4b2074645ffb3da12/`
contains the graphs/catalog/change decisions and `live-check.json`;
the `logs/` subdirectory contains events and final metrics/manifests.
These are local temporary artifacts, not committed copies of the user's source.

## Automated checks after the local fixes

- Full suite with trusted private SDK installed: **206 passed**.
- Private SDK deliberately blocked through an import hook: **160 passed, three
  SDK-dependent modules skipped**. This checks the no-SDK runtime boundary; it is
  not a newly provisioned isolated dependency environment.
- Ruff and whitespace checks: passed.
- Release recheck: **206 tests passed**, Ruff and whitespace checks passed, and
  the credential-pattern scan found no matching keys. Offline `uv build` built
  both the source distribution and wheel successfully using the local cache.
- The real SDK plus mocked HTTP tests verify model/base-path/auth/custom headers,
  both AI and legacy-description flows, client cleanup, errors, and no redirect/retry.
- Existing mode tests cover off/fallback/improve, shared descriptions, background
  immutability, budgets, timeouts, evidence validation, audit failures and rollback.
- New help tests exercise all command/group paths; a README test checks every
  registered option is documented; the catalog template is parsed and used in a scan.

## Caching clarification — no new cache feature added

Requests are separate per selected file in `run`, and per eligible column in legacy
`describe`; lineage/descriptions can share a file request. There is no application
response cache or explicit prompt-cache configuration. Provider/gateway-side caching
may happen, but these live responses reported zero cache-read/write tokens through
the SDK. That does not prove the gateway never cached or reported every upstream field.

Caching is optional optimization, not necessary for correct analysis. Official OpenAI
documentation confirms supported models enable prompt caching by default with matching
prefix/model-specific rules; gateway translation may differ. This clarification was
checked using the OpenAI Docs skill and the
[official prompt-caching guide](https://developers.openai.com/api/docs/guides/prompt-caching).
The user asked about caching, then said to leave it; no cache policy was enabled.

## Remaining live validation

The workspace approval service rejected the additional live fallback request because
the workspace was out of credits. This is **not** a gateway authentication or gateway
billing error. No workaround was used to bypass that rejection.

Approval succeeded for the later offline release build. The user subsequently
requested committing and pushing the implementation; no additional paid gateway
requests were made during that release pass. Publishing these changes does not
close the following live-validation gaps:

1. Live-retest the fixed multi-proposal validation path and the seven failed/skipped files.
2. Complete the separately planned fallback-only and background-only live checks.
   Dry-run selects exactly four fallback files; mocked tests cover both policies already.
The release gates above have been re-run successfully. The previous release was
`81f569d`; this report accompanies the runner-selection follow-up changes.
