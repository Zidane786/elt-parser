# Follow-up: selectable SDK runners, complete usage docs and live gateway tests

Status: implemented; release checks passed; live validation remains partial.
The user requested committing and pushing the implementation with the remaining
live checks documented. See [validation report](../../reports/2026-09-18-gateway-validation.md).

## User decisions

- [x] Support `AnthropicRunner` and Lambda Bedrock Invoke through the existing private SDK.
- [x] Name the Lambda option `lambda-bedrock-invoke`, with `lbi` as an alias, not `bedrock`.
- [x] Expose base URL, API key and optional JSON custom headers for Anthropic.
- [x] Use the user's selected gateway/model with **no extra headers** for live tests.
- [x] Keep secrets out of files/logs/commits; use hidden terminal input for live checks.
- [x] Test the real ETL folder and all AI policy combinations in automated tests.
- [x] Detailed README covering every command and available option, plus `--help` everywhere.
- [x] Valid `catalog.template.json` with explanation of schema versus prior metadata.
- [x] Explain buffered/non-streaming Lambda ARN usage; deployment has separate streaming ARN.
- [x] Explain per-file requests and cache-read/write accounting; leave cache tuning unchanged.

## Implementation and validation

- [x] Shared runner configuration/factory with conditional provider initialization.
- [x] API-key/header exclusion from serialized configs, validation and log redaction.
- [x] Runner options in both `run` and `describe`; factory-owned HTTP-client cleanup.
- [x] Real SDK/mock HTTP contract tests, aliases, bad headers, auth errors, redirect rejection.
- [x] Help tests, README-option coverage and template schema/export validation.
- [x] One live synthetic combined-lineage/description request passed.
- [x] Bounded live corpus run attempted: 25 calls, 21 validated files, four failures,
  three deadline skips. Preserve partial status and all baseline evidence.
- [x] Fix the live-discovered dataset/column-schema variable collision with regression test.
- [x] Add sanitized schema-error locations/types and response digests for diagnosis.
- [x] Full local tests, SDK-blocked tests, lint and whitespace checks passed.
- [x] Live-retest corrected code and failed/skipped files with the subsequently selected
  replacement provider; all seven files returned valid responses across one run and one retry.
- [x] Live fallback-only/background-only calls completed; retained partial
  analysis and rejected proposals are detailed in the
  [follow-up report](../../reports/2026-09-19-live-validation.md).
- [x] Final release checks: 206 tests, lint, whitespace, credential-pattern scan,
  and offline source/wheel builds passed.

Release instruction: commit and push this follow-up, then verify the remote branch.
The follow-up checks exercise request modes, not exhaustive parser/model accuracy.
