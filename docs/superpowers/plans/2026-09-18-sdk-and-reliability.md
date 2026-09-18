# SDK and reliability follow-up

User request: fix the remaining problems, use a configurable 16,000-token default,
support CLI and a backend Python SDK, then commit and push.

- [x] Raise analysis/description defaults and retain CLI/Python overrides.
- [x] Increase configurable request timeout and AI-stage deadline.
- [x] Distinguish deadlines, call limits, truncated responses, schema and provider errors.
- [x] Stop remaining AI requests after authentication/endpoint/rate-limit failures.
- [x] Public sync/async client, isolated per-request state and serializable results.
- [x] Offload scanning/export from the service event loop; finalize logs on errors/cancellation.
- [x] Keep baseline preservation and strict response/evidence validation.
- [x] Add regression, concurrency, cancellation, token-control and failure-path tests.
- [x] Scan the real ETL folder through the SDK; verify fallback selection without paid calls.
- [x] Update README and provide a plain-language delivery/validation report.
- [x] Run full tests, SDK-unavailable tests, lint and source/wheel builds.
- [x] Attempt targeted live retests; diagnose HTTP 429 and stop further requests.
- [ ] Obtain successful seven-file and fallback/background live revalidation after the gateway limit clears.

Release instruction: commit and push the implemented changes with the live blocker
explicitly documented; do not mark the remaining live validation complete.

Report: [SDK and reliability](../../reports/2026-09-18-sdk-and-reliability.md).
