# Dependencies

Every dependency here is small, widely used and justified below. **Before a package is
added, it must resolve on your organisation's internal package index** — the one your
pip/uv configuration points at. A requirement that cannot be mirrored there is not
adopted, whatever its merits. Adding one also pulls its transitive requirements, so check
the resolved lock file (`uv.lock`), not just the name you typed.

No provider SDK is a runtime requirement: lineage never calls a model, and the AI layer
imports your organisation's Agent SDK (`agent-sdk`) lazily, only when an AI stage is
explicitly enabled.

## Runtime requirements

Declared in `pyproject.toml` under `[project].dependencies`; installed by a plain
`pip install .`.

| Package | Purpose | Where it is imported |
| --- | --- | --- |
| `pydantic>=2.5` | Every lineage type, the strict AI response schema and runner/analysis settings; gives validation and deterministic JSON for free | `etl_parser/models.py`, `ai_analysis.py`, `describe/client.py`, `scanner/references.py` |
| `sqlglot>=30` | SQL parsing, dialect handling, qualification and column-level lineage; the core of SQL and embedded-SQL analysis | `etl_parser/workers/sql.py`, `workers/python.py` |
| `networkx>=3.2` | Dataset/column graph storage and the traversals behind impact and product dependencies | `etl_parser/graph/builder.py`, `graph/impact.py`, `describe/engine.py` |
| `pyyaml>=6` | Reading `product.yaml` registries | `etl_parser/registry.py` |
| `typer>=0.12` | The command-line interface, its help text and parameter validation | `etl_parser/cli.py` |
| `openlineage-python>=1.30` | Building spec-correct OpenLineage events and column-lineage facets instead of hand-rolled JSON | `etl_parser/export/openlineage_out.py` |

Only the standard library is used elsewhere: `ast` for Python analysis, `zipfile` for ZIP
helper libraries, `urllib` for the GitHub source, `logging`/`json` for observability.

## Optional extras

Installed on demand, e.g. `pip install '.[glue]'`. Their drivers are imported **inside**
the function that needs them, so importing `etl_parser` never requires a database client
and a deterministic scan never touches a network.

| Extra | Package | Purpose |
| --- | --- | --- |
| `glue` | `boto3>=1.34` | Reading databases, tables, columns and partition keys from the AWS Glue Data Catalog |
| `postgres` | `psycopg[binary]>=3.1` | Reading `information_schema`, column comments and primary/foreign keys from PostgreSQL |
| `redshift` | `redshift_connector>=2.1` | The same for Redshift (`svv_columns`, `svv_table_info`, constraints), including IAM authentication |

Missing an extra is reported as a clear `ImportError` naming the extra to install; it is
never a silent fallback to guessed schema information. Credentials come from the
environment or your normal AWS credential chain, never from a command-line option.

## Development tools

Declared under `[dependency-groups].dev`; not installed for consumers.

| Tool | Purpose |
| --- | --- |
| `pytest>=8` | The test suite, including the fixture corpora and golden comparisons |
| `ruff>=0.6` | Linting (`E`, `F`, `I`, `UP`, `B`) and formatting at a 100-column line length |

`uv` is used for environment and lock management (`uv sync`, `uv build`); it is a tool,
not a dependency of the package.

## Considered and not used

| Package | Why not |
| --- | --- |
| `grimp` | Import-graph analysis for module dependencies; the scanner already builds the module index it needs from `ast`, so this added a dependency for a graph we compute in a few lines |
| `libcst` | Concrete syntax trees preserving formatting; lineage never rewrites source, so the stdlib `ast` is sufficient and one dependency lighter |
| `jedi` | Static completion/inference for Python; it is tuned for editor completion rather than dataflow, and its inference would blur the exact/inferred confidence boundary the design depends on |
| `sqllineage` | Ready-made SQL lineage; it is table-level first, does not expose the per-column evidence (expression, line range, provenance) the catalog requires, and would duplicate the `sqlglot` parse we already run |
| `sqlglotc` | An optional native accelerator for `sqlglot`; a compiled wheel to mirror internally for a parse cost that is not the bottleneck |
| `astroid` | Was declared but never imported (review finding 26); `ast` plus the scanner's own frame tracking covers the inference needed, so it was removed |
| `anthropic` | A provider SDK would tie the package to one vendor; the LLM client stays pluggable and the AI layer talks only to your organisation's Agent SDK (`agent-sdk`), imported lazily |

Removing a dependency is treated like adding one: the reason is recorded here so the same
package is not re-proposed later.
