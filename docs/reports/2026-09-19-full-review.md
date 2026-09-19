# Full review of etl-parser (2026-09-19)

Scope: every module under `etl_parser/`, every test, every document under `docs/`, the
original design spec and plan, the AI/observability/GitHub follow-up spec and plan, the three
prior reports, and a live run against the user's Anthropic-compatible gateway through the
private Agent SDK. Four independent read-only review streams (Python and scanner workers;
SQL, Airflow and string folding; graph, exporters and CLI; AI analysis, SDK, sources and
observability) plus a coordinating pass that reproduced every High finding before it was
recorded here. No production code was changed by this review. Docstrings were added
separately (see "Documentation" at the end).

Baseline at review start: commit `9c0c32a`, 226 tests passing, ruff clean.

## 1. Verdict in one paragraph

The deterministic core does what the design promised on the fixture corpora, the AI layer is
off by default and never overwrites deterministic edges, and no secret reached any artifact
or log during the live run. The package is not yet safe to hand to another team as-is. Three
classes of defects would mislead a consumer without any warning: the agent catalog export
duplicates databases and drops prior metadata whenever the prior catalog's `db_type` is not
a Glue engine (the user's own catalog uses `sqlite`); several common Spark, Airflow and SQL
patterns are silently dropped rather than reported as unresolved; and the in-place-writer
rule removes real dependencies. Each is fixable in isolation and each has a repro below.

## 2. Requirement coverage

| Requirement (source) | Status | Notes |
| --- | --- | --- |
| Deterministic lineage, no LLM in the lineage path (original brief) | Holds | AI lineage is opt-in (`--ai-lineage fallback|improve`); AI edges carry `parser=agent_sdk_ai`, `confidence=inferred`, model id, request id, evidence digest. Live run: 177 deterministic edges and 8 diagnostics intact after 4 inferred additions. |
| AI only for descriptions, grounded in the transformation snippet (brief) | Holds with drift | Prompt includes the verbatim edge JSON. `domain` is never passed; `business_rule`/`confidence` are requested but never read; edge provenance fields leak into descriptions as jargon. |
| Column-level edges with expression, file, line range, provenance, confidence (spec 2) | Holds for SQL; partial for Python | `inferred` is never emitted by any deterministic worker; helper-followed edges report `exact`. Embedded SQL line offsets are anchored to the call, not the string literal. |
| Unresolved items first-class, never guess (spec 3) | Violated in several paths | See section 3, "Silent drops". |
| Native JSON + agent `catalog.json` + OpenLineage (spec 4) | Partial | Native and OpenLineage exports exist and are deterministic. Catalog export has the High defects in section 3. OpenLineage INDIRECT subtypes are always null. |
| Impact assessment, cross-product flag (spec 5) | Partial | Dataset walk correct. Column walk never flags cross-product edges. Job reads without a table edge are invisible to impact. |
| Product graph derived vs declared, drift (spec 6) | Holds | reg→bill and reg→meter confirmed; declared-but-unobserved reported. Product attach ignores database engine type. |
| Pluggable LLM client, no provider SDK dependency (spec 7) | Holds | Only `agent_sdk`, imported lazily. `anthropic` is never imported. User confirmed SDK error types are the desired failure surface. |
| Airflow schedules dict, DAG-declared depends_on (session asks) | Partial | Context-manager and decorator DAGs work. Constructor DAGs with `dag=` are misattributed; loop-generated tasks vanish. |
| depends_on = data ∪ dag, in-place writer excluded (session asks) | Implemented, rule too broad | `dim_customer_scd2` is the only writer of `dim_customer` yet is excluded as a dependency of `mart_cohort_retention` and `mart_customer_ltv`; the golden fixture froze this regression. |
| Output byte-identical across runs (spec 13) | Holds | `cmp` identical on both fixtures; `generated_at` never populated. Intermediate frame order depends on set iteration but the sorted document hides it. |
| Dependencies only via Nexus, justified (user rule) | One unused dependency | `astroid` is declared in `pyproject.toml` and never imported. |
| Public GitHub repo for other teams | Attention needed | Internal names appear in the public repo: `gdtc-agent-sdk`, `x-duke-*` header examples, Nexus. |

## 3. Defects, ranked

Severity reflects consumer impact: High means a consumer receives wrong or incomplete data
with no signal.

### High

1. **Catalog export duplicates databases when prior `db_type` is not a Glue engine.**
   `export/agent_catalog.py` matches prior databases by name *and* scheme. The user's real
   catalog uses `db_type: sqlite`; the export emits `ecommerce`/`analytics_warehouse` twice
   (sqlite copy with prior metadata, glue copy with new columns). 946 prior field entries are
   not merged. In the live run all seven AI descriptions landed in the glue duplicate.
   Repro: `etl-parser export catalog lineage.json --prior tests/fixtures/etl_catalog_original.json`.
   Fix: match on `db_name` first; treat a single same-name prior database as the target
   regardless of engine; never emit `db_type: glue` (use `athena`). Add a test that uses the
   real original catalog as `--prior`.

2. **Prior script descriptions lost when the scan root differs from the catalog path prefix.**
   Prior scripts are matched by `job_id` or exact `script_path`. Prior `etl/stage_orders.py`
   versus scanned `stage_orders.py` matches nothing, so all 28 descriptions are dropped and
   unmatched prior scripts disappear. Fix: fall back to `script_name`, then path suffix; keep
   unmatched prior scripts as untouched entries.

3. **In-place-writer rule removes sole-producer dependencies.** `graph/builder.py` skips a
   data dependency whenever the upstream job also reads the dataset. Correct for
   `purge_pii_after_retention`; wrong for `dim_customer_scd2`, which is the only writer of
   `dim_customer`. Fix: exclude an in-place writer only when the dataset has at least one other
   writer; otherwise add the dependency with `in_place_writer=True` as information. The golden
   fixture must be regenerated after the fix.

4. **Chained Spark writers silently dropped.** `df.write.mode("overwrite").parquet(path)`,
   `.option(...).csv(path)`, `.partitionBy(k).json(path)` produce `outputs=[]` and no
   unresolved item, because sink matching needs the `write.parquet` suffix and only readers
   have a builder fallback. Reproduced: inputs `[glue://shop/orders]`, outputs `[]`,
   unresolved `[]`. Fixtures only use `saveAsTable`, so the golden missed it.

5. **Constructor-style DAGs: `dag=` keyword ignored.** Tasks are attributed to the last
   constructed DAG. Reproduced: `BashOperator(..., dag=dag1)` reported under `second`.

6. **Tasks created inside `for`/`while`/`try`/comprehensions/`.expand()` vanish with no
   diagnostic.** Reproduced: two loop-generated tasks absent from `schedules` and `task_jobs`.

7. **One Jinja placeholder discards the whole SQL script.** A `{{ ds }}` in a `WHERE` literal
   yields a single `dynamic_sql` at line 1 and drops every statement, including clean ones.
   Reproduced on a two-statement file. Spec 8.2 step 3 required placeholders in
   non-identity positions to pass through with `partial` confidence.

8. **RecursionError escapes the Python worker** on very long expression chains (roughly 400
   `+` terms), because the diagnostic path calls `ast.unparse` on the same node inside the
   handler. Violates the never-raise rule.

### Medium

9. AI-proposed datasets and job inputs/outputs enter `lineage.json` without provenance;
   only the edges are marked. A file with only diagnostics can gain a brand-new unmarked job.
10. Evidence validation accepts a one-character quote (`min_length=1`, substring check), and
    dataset validation is a substring match, so `db.trans` validates against `db.transactions`.
11. Column-level impact never reports cross-product edges (intersects table edges with column
    node ids). Dataset-level impact always returns `columns: []`.
12. Impact ignores job reads that have no table edge (`mart_funnel_conversion` reads
    `fact_sessions`; `impact ... fact_sessions` returns nothing).
13. OpenLineage INDIRECT transformations have `subtype: null`; the edge kind is available.
    Field-level `transformationType` emits a non-spec value `EXPRESSION`.
14. Temp tables are expanded at column level but remain in `inputs`, `outputs`, `datasets` and
    `table_edges`, so phantom `glue://default/tmp_*` datasets reach the catalog.
15. Embedded-SQL line numbers are anchored to the call, not the string literal (Airflow and
    Python paths), so reported lines are off by the distance between the two.
16. Airflow variable reuse resolves to the last assignment (`t=one; t>>u; t=three; u>>t`
    yields a cycle and isolates `one`).
17. `analyze_file` raises `ValueError` when the path is outside the index root.
18. Fake column names reach edges: `{{?}}`, `*`, and `''` appear as target or source column
    names instead of `unknown_column` items.
19. Reads inside unevaluated expressions are lost with no diagnostic: arguments of unknown
    calls, `for row in spark.table(...).collect()`, tuple targets, `try` handlers, class bodies.
    `AugAssign` is ignored, so `t='a.'; t+='b'` fabricates `glue://default/a`.
20. A `return` nested inside `if`/`for`/`with` in a helper is discarded; the helper returns the
    fall-through frame.
21. Provider misconfiguration (missing Lambda ARN, SDK not installed) is caught by the per-file
    handler and reported as `provider_or_response_failure`, indistinguishable from an outage.
    `docs/sdk.md` says invalid settings raise.
22. GitHub source: symlinks and submodules become `unsupported_syntax`, so `scan` exits 1 on
    any repo containing one; local scans skip them silently. No circuit breaker: after a
    rate-limit failure every remaining file still issues a request.
23. `scan` exit code over-fires: the `products` fixture exits 1 from fifteen heuristic
    "Dynamic control flow analyzed conservatively" notes, so CI gating is not usable as
    documented.
24. Legacy `describe` prints nothing on success and `run` exits 0 on HTTP 401 with status
    `partial`. An authentication failure should be visible without `--strict`.
25. `SELECT *` over a join with schema emits duplicate output names; `UPDATE ... FROM` with
    `JOIN` loses the join's indirect sources.

### Low

26. Unused dependency `astroid`. `grimp`, `libcst`, `jedi` were dropped from the plan without a
    note in `docs/dependencies`.
27. `normalize_dataset_id("")` and `"."` raise `IndexError`; `split_dataset_id("no-scheme")`
    raises `ValueError`; callers are unguarded.
28. `depends_on` uses `script_name`, ambiguous across products (`gen_data`, `load_to_athena`).
29. `in_place_writer` flag is per job, not per linking dataset.
30. Product attach ignores `ProductDatabase.type`; a Postgres `meter_cur` gets product `meter`.
31. Meter `product.yaml` uses `schedules.scripts:`; the registry reads only `steps`, so those
    schedules are silently ignored.
32. Sink suffixes `load`, `save`, `execute`, `text` can match unrelated calls
    (`json.load`, `np.save`, `soup.text`); only `load`/`save` are receiver-guarded.
33. Language detection by substring `"pyspark" in source` misclassifies Glue jobs and pandas
    files that mention PySpark in a comment. `SparkStaticWorker` is an alias of
    `PythonWorker` and never sets `parser=spark_static` on its own.
34. `spark.read.parquet(path=...)` keyword form is dropped; `Connection.cursor()` is not
    followed so dialect is lost.
35. Missing-file errors in `export`, `impact`, `describe` surface as raw tracebacks;
    `export` does not create the `--out` parent directory.
36. Local `.zip` files skip the size check and are decompressed in one read, trusting the
    header size.
37. `GITHUB_TOKEN=""` shadows `GH_TOKEN`, contrary to the SDK guide.
38. `scanner/imports.py::resolve_import` and `sql.py::_all_selects` have no callers;
    `callee()` is duplicated between `python.py` and `references.py`.
39. Console output: every command streams JSON events to stderr at INFO by default, including
    when `etl_parser.scan()` is used as a library. A library default of WARNING (or a
    `NullHandler`) would be conventional.
40. Header examples `x-duke-mode`/`x-duke-stream`, the `gdtc-agent-sdk` name and Nexus
    references live in a public repository.

## 4. What the live gateway run showed

Four bounded runs through the SDK `AnthropicRunner` against the user's gateway, credentials
supplied only through the environment.

| Run | Calls | Outcome |
| --- | ---: | --- |
| Descriptions only, lineage off, 2 files | 2 | One valid response (7 descriptions marked `ai`). One response was not JSON and was rejected, not guessed. Deterministic graph unchanged. |
| Fallback lineage, no descriptions | 2 | 4 inferred edges accepted with model/request/evidence provenance; 2 deferred; 177 deterministic edges and 8 diagnostics preserved. |
| Legacy `describe`, single file | 0 | All 15 columns skipped because prior descriptions existed. No output printed. |
| Invalid key | 1 | HTTP 401 → `provider_authentication_failed`, exit 0, status `partial`. |

Secret scan: the key fragment appears in no artifact, log, or stdout. The gateway hostname
appears in `manifest.json` under `configuration` in every run; treat manifests as
configuration-sensitive.

Description quality: the model echoed provenance jargon ("exact-confidence static Spark
lineage") because the prompt serialises whole edge objects. Strip `provenance` and
`job_id` from the prompt payload and pass `domain` and source datatypes as the spec intended.

## 5. Claims in earlier reports that do not hold

- `docs/implementation-review.md`: "Context/constructor/decorator DAGs" — constructor DAGs
  are wrong whenever `dag=` selects a non-last DAG. "Unsupported call/control-flow
  diagnostics" — AirflowWorker emits none for loops or comprehensions. "Temp tables" —
  collapsed at column level only. "SQL templates support simple named substitutions" — any
  unbound `{{ }}` discards the whole script.
- `docs/cli.md`: `--prior` "preserves descriptions/metadata" — only when prior `db_type` is a
  Glue engine or empty. `scan` exits 1 for `unsupported_syntax` — true, but heuristic notes
  use that kind, so the shipped `products` fixture fails.
- `docs/sdk.md` line 101: "Invalid settings raise" — provider misconfiguration is reported
  per file, not raised.
- README section 3.2 and spec: columns in the downstream closure — dataset walks return
  `columns: []`.

## 6. Recommended order of work

1. Catalog export: database matching, script matching, `description_source` on every
   description, a golden test with the real original catalog as prior. (Defects 1, 2)
2. Dependency rule: sole-writer exception; regenerate golden. (3)
3. Python worker: chained writer fallback, recursion guard, `relative_to` guard, reject
   placeholder column names, evaluate `for.iter`/call args/tuple targets/`try`/`AugAssign`,
   nested returns, emit `inferred` for helper-followed and branch-merged frames. (4, 8, 17–20)
4. Airflow: honour `dag=`, visit control flow with diagnostics, bind symbols at assignment,
   `@task.*` variants, TaskGroup prefix. (5, 6, 16)
5. SQL: Jinja placeholders pass through with `partial`, keep clean statements, exclude temp
   tables from datasets, anchor line offsets to the string literal. (7, 14, 15, 25)
6. Impact and OpenLineage: cross-product for column walks, job I/O fallback edges, INDIRECT
   subtypes. (11–13)
7. AI layer: mark AI-added datasets/jobs, tighten evidence (minimum quote length, identifier
   match), pre-flight provider config, prompt hygiene. (9, 10, 21)
8. CLI and library ergonomics: non-gating kind for heuristic notes, non-zero exit on auth
   failure, `describe` summary line, WARNING default for library logging, typer errors for
   missing files. (23, 24, 35, 39)
9. Hygiene before the next public push: remove `astroid`, scrub internal names and header
   examples, add a `docs/dependencies.md` explaining dropped packages. (26, 40)

## 7. Documentation

Google-style docstrings were added to every module, class, function and method in
`etl_parser/` (previously 30/36 modules, 7/65 classes, 35/216 functions). Four Sonnet agents
wrote them on disjoint file sets under a docstrings-only rule; the coordinating pass verified
zero missing definitions, ruff clean, the full suite green, and cross-checked every `Raises:`
section against actual `raise` statements (five internally-caught raises are correctly
omitted). Design-doc section references in module docstrings were checked against the spec.

---

## 8. Resolution (same day)

Every finding above was worked in seven parallel packages, each on its own branch with
tests written first, then merged into one integration branch. The suite grew from 226 to
531 tests. `ruff check`, `ruff format --check`, byte-identical double scans of both
fixtures under different hash seeds, and an import guard proving a scan works with
`agent_sdk`, `anthropic`, `openai`, `psycopg` and `redshift_connector` all blocked, all pass.

| Finding | Resolution |
| --- | --- |
| 1 Catalog duplicated databases | Prior databases match on name first; `db_type: glue` is never emitted. Verified against the real catalog: 2 databases, all 321 columns and 47 relations preserved. |
| 2 Prior script descriptions lost | Matched by job id, then exact path, then script name, then path suffix. Unmatched prior scripts are kept. |
| 3 In-place writer dropped real dependencies | A job whose every input is also an output is a pure mutator and creates no dependency; a job reading anything it does not write stays a producer. Golden diff is exactly the two `dim_customer_scd2` additions. |
| 4 Chained Spark writers dropped | `write.mode(...).parquet(path)` and friends resolve, including `option("path", ...)`. |
| 5 Constructor DAGs misattributed | Tasks bind by `dag=` and `with dag_var:`; two DAGs in one file stay separate. |
| 6 Loop-generated tasks vanished | `for`/`while`/`try`/comprehensions are visited; literal loops unroll, unknown iterables raise `dynamic_schedule`. |
| 7 One Jinja placeholder discarded a script | Placeholders in value position are analysed at `partial` confidence with the note at the hole's line; clean statements are kept. |
| 8 RecursionError escaped | Caught and reported; the job is still emitted. |
| 9 AI datasets and job I/O unmarked | Datasets carry `origin="ai"` with provenance; accepted reads and writes go to `ai_inputs`/`ai_outputs` and never touch deterministic inputs. |
| 10 Evidence trivially satisfiable | Quotes need 12+ characters and must name the target column or an identifier from the expression; dataset grounding is whole-token. |
| 11-13 Impact and OpenLineage gaps | Column walks report cross-product edges, dataset walks list columns, `job_io` fallback edges make declared reads reachable, INDIRECT subtypes are populated. |
| 14-16, 25 SQL and Airflow gaps | Temp tables leave the catalog, SQL line numbers anchor to the string literal, variable reuse no longer cycles, more statement forms modelled. |
| 17-20 Python worker silent drops | Guards for paths outside the root, placeholder column names rejected, unevaluated positions walked, nested returns respected, helper and branch frames marked `inferred`. |
| 21 Provider misconfiguration looked like an outage | Pre-flight raises `provider_configuration_invalid`; other failures carry `internal_error`. |
| 22 GitHub symlinks gated the scan | Now `skipped_entry`, with a breaker after three consecutive failures. |
| 23-24 Exit codes | Only `unsupported_syntax` gates; authentication and authorization failures exit 2; `describe` prints a summary. |
| 26, 40 Hygiene | `astroid` removed, extras added for Postgres and Redshift, `docs/dependencies.md` added, example header names and the private SDK reference made generic. |
| 27-39 Remaining low findings | Sentinel ids instead of exceptions, per-dataset in-place flag, engine-preferring product ownership, both product YAML schedule shapes, guarded sink suffixes, language detection by imports, bounded ZIP reads, dead code removed. |

Beyond the findings, this pass added what the user asked for during the work: `ai_confidence`
and `ai_rationale` on every AI edge and description, `min_ai_confidence` as a floor, table
descriptions generated alongside their columns in one call, `--generate` and `--database`
selection on the CLI and the SDK, source-of-truth schema fetching from Glue, Postgres and
Redshift with a `schema fetch` command, a `schema_drift` block reporting what exists in code
but not in the source system, and inferred relations from join conditions kept separate from
declared ones.

### Live gateway validation of the merged branch

Six scenarios through the Agent SDK against the user's Anthropic-compatible gateway, with
credentials supplied only through the environment.

| Scenario | Result |
| --- | --- |
| Descriptions for a table with real gaps | One call covered the table and its columns. Table description confidence 0.85, column 0.7. 12 identity columns inherited without a model call. No parser or lineage jargon in the text. |
| AI lineage, fallback mode | One inferred edge accepted at confidence 0.96 with a rationale. 177 deterministic edges and all 9 diagnostics preserved. |
| `--min-ai-confidence 0.99` | The same 0.96 proposal was deferred. Effective graph identical to the deterministic baseline. |
| `--generate scripts --database ecommerce` | Selected sections regenerated, unselected ones passed through from the prior. |
| Invalid credentials | Exit code 2, reported as an authentication failure. |
| Secret scan over every artifact and log | The key appears nowhere. |
