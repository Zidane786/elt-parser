"""Catalog export preserving prior human metadata and flags (spec sections 3.1, 10).

Implements the ``AgentCatalogExporter`` role: writes ``catalog.json`` in the exact shape
``de_agent`` expects (top-level ``databases``, ``relations``, ``scripts``), merging in a
prior catalog when supplied so human/AI descriptions and catalog flags (``to_tokenize``,
``verify``, etc., which are not derivable from code) survive re-scans. Also writes the
``schedules`` and ``lineage`` extension fields the current agent ignores.
"""

import copy

from etl_parser.identity import ENGINE_SCHEME, GLUE_ENGINES, agent_table_name, split_dataset_id
from etl_parser.models import Unresolved
from etl_parser.observability import current_observer


def _database_scheme(database):
    """Return the identity scheme (e.g. ``glue``) implied by a catalog database's type.

    Args:
        database: A catalog ``databases[]`` entry; its ``db_type`` is inspected.

    Returns:
        str: ``glue`` for Athena/Spark/Glue engines, ``""`` when no type is recorded,
        else the engine scheme or the raw lower-cased type.
    """
    kind = (database.get("db_type") or "").lower()
    return "glue" if kind in GLUE_ENGINES else ENGINE_SCHEME.get(kind, kind)


def _db_type(scheme):
    """Return the ``db_type`` to write for a database created from a dataset scheme.

    Glue-scheme datasets are recorded as ``athena`` because ``glue`` is an identity
    scheme, not an engine the agent knows.

    Args:
        scheme: The dataset id scheme (``glue``, ``postgres``, ...).

    Returns:
        str: The engine name to store in ``db_type``.
    """
    return "athena" if scheme == "glue" else scheme


def _same_db_name(prior_name, namespace, scheme):
    """Tell whether a prior database name denotes the dataset namespace.

    Glue identifiers are case-insensitive, so glue datasets match prior names ignoring
    case; every other scheme compares exactly.

    Args:
        prior_name: ``db_name`` of a prior catalog database.
        namespace: Namespace part of the dataset id.
        scheme: Scheme part of the dataset id.

    Returns:
        bool: True when the names denote the same database.
    """
    if scheme == "glue":
        return (prior_name or "").lower() == namespace.lower()
    return prior_name == namespace


def _find_database(databases, namespace, scheme):
    """Pick the catalog database a dataset belongs to, matching by name first.

    A single database with the dataset's name is the target regardless of its
    ``db_type`` (the source system, not the code, decides the engine). When several
    share the name, the one whose type maps to the dataset scheme wins, then one with no
    type; otherwise there is no match.

    Args:
        databases: The catalog ``databases`` list.
        namespace: Namespace part of the dataset id.
        scheme: Scheme part of the dataset id.

    Returns:
        dict | None: The matching database entry, or ``None`` when a new one is needed.
    """
    candidates = [d for d in databases if _same_db_name(d.get("db_name"), namespace, scheme)]
    if len(candidates) == 1:
        return candidates[0]
    for wanted in (scheme, ""):
        for database in candidates:
            if _database_scheme(database) == wanted:
                return database
    return None


def _mark_description(entry, source="human"):
    """Stamp ``description_source`` on an entry that has a description but no source.

    Entries whose source is already recorded (``ai``, ``inherited``, ``code``, ...) are
    left untouched, so AI keys such as ``ai_confidence`` and ``ai_model`` survive as they
    were written. Empty descriptions get no source.

    Args:
        entry: A database, table, column or script dict.
        source: The source to record when none is present.
    """
    if entry.get("description") and not entry.get("description_source"):
        entry["description_source"] = source


def _mark_human_descriptions(databases):
    """Mark every prior database, table and column description without a source as human.

    Args:
        databases: The catalog ``databases`` list, mutated in place.
    """
    for database in databases:
        _mark_description(database)
        for table in database.get("tables", []):
            _mark_description(table)
            for column in table.get("schema", []):
                _mark_description(column)


def _script_description(old, job):
    """Choose a script's description and its source.

    Prior text written by a person or the AI layer is kept; text this exporter derived
    from code earlier is refreshed from the job's docstring, as is an empty description.

    Args:
        old: The matched prior script entry (possibly empty).
        job: The scanned :class:`~etl_parser.models.Job`.

    Returns:
        dict: ``{"description": str}`` plus ``description_source`` when the text is
        non-empty.
    """
    text = old.get("description") or ""
    source = old.get("description_source")
    if text and source != "code":
        return {"description": text, "description_source": source or "human"}
    if job.description:
        return {"description": job.description, "description_source": "code"}
    return {"description": ""}


def _relations_inferred(join_conditions):
    """Turn observed equality joins into ``relations_inferred`` entries.

    A join has no direction, so the two sides are ordered lexicographically by
    ``(table, column)`` and both orientations collapse into one entry that lists every
    job in which the join was seen. The result is kept apart from ``relations``:
    referential integrity is a data-model fact the source system owns, this is only
    evidence from code.

    Args:
        join_conditions: :class:`~etl_parser.models.JoinCondition` items from the document.

    Returns:
        list[dict]: Sorted entries ``{from_table, from_column, to_table, to_column,
        relation_type: "join", source: "inferred", jobs: [...]}``.
    """
    grouped: dict[tuple[str, str, str, str], set[str]] = {}
    for condition in join_conditions:
        sides = sorted(
            (
                (agent_table_name(condition.left.dataset_id), condition.left.name),
                (agent_table_name(condition.right.dataset_id), condition.right.name),
            )
        )
        key = (sides[0][0], sides[0][1], sides[1][0], sides[1][1])
        grouped.setdefault(key, set()).add(condition.job_id)
    return [
        {
            "from_table": from_table,
            "from_column": from_column,
            "to_table": to_table,
            "to_column": to_column,
            "relation_type": "join",
            "source": "inferred",
            "jobs": sorted(jobs),
        }
        for (from_table, from_column, to_table, to_column), jobs in sorted(grouped.items())
    ]


def _referencing_jobs(doc, dataset):
    """Return the jobs that read or write a dataset (by id or alias), sorted by id.

    Args:
        doc: The :class:`~etl_parser.models.LineageDocument` being exported.
        dataset: The :class:`~etl_parser.models.DatasetRef` to look up.

    Returns:
        list[Job]: Referencing jobs in id order.
    """
    ids = {dataset.id, *dataset.aliases}
    return sorted(
        (j for j in doc.jobs if ids & set(j.inputs) or ids & set(j.outputs)), key=lambda j: j.id
    )


def _record_code_only(drift, scheme, namespace, name, columns, jobs):
    """Record a table (or the code-only columns of a known table) in the drift collector.

    Args:
        drift: ``{(db_name, scheme): {table_name: table_entry}}`` accumulator.
        scheme: Dataset id scheme.
        namespace: Dataset namespace (database name).
        name: Table name.
        columns: Column names seen only in code.
        jobs: Jobs referencing the dataset, sorted by id.
    """
    drift.setdefault((namespace, scheme), {})[name] = {
        "table_name": name,
        "description": "",
        "schema": [
            {"field_name": column, "datatype": None, "description": ""}
            for column in sorted(columns)
        ],
        "referenced_by": [job.id for job in jobs],
        "source_files": sorted({job.source_file for job in jobs}),
    }


def _missing_in_source(dataset_id, symbols, jobs, whole_table):
    """Build the non-gating ``missing_in_source`` item for a code-only table or columns.

    Args:
        dataset_id: Canonical id of the dataset referenced in code.
        symbols: The dataset id (whole table) or the code-only column names.
        jobs: Jobs referencing the dataset, sorted by id; the first anchors the item.
        whole_table: True when the table itself is absent from the source schema.

    Returns:
        Unresolved: The item, ready to be serialized into ``lineage.unresolved``.
    """
    first = jobs[0] if jobs else None
    reason = (
        f"{dataset_id} is referenced in code but absent from the source schema"
        if whole_table
        else f"{dataset_id}: columns referenced in code are absent from the source schema"
    )
    return Unresolved(
        kind="missing_in_source",
        source_file=first.source_file if first else None,
        job_id=first.id if first else None,
        reason=reason,
        symbols=list(symbols),
        assumptions={"referenced_by": ", ".join(job.id for job in jobs)},
        remediation=(
            "Verify against the source system and add it to the catalog, or export with "
            "include_code_schema=True to add the code-derived entry marked schema_source: code."
        ),
    )


def _drift_databases(drift):
    """Render the drift collector in the ``databases`` shape, sorted.

    Args:
        drift: ``{(db_name, scheme): {table_name: table_entry}}`` accumulator.

    Returns:
        list[dict]: One database per key with its tables ordered by name.
    """
    return [
        {
            "db_name": namespace,
            "db_type": _db_type(scheme),
            "description": "",
            "tables": [tables[name] for name in sorted(tables)],
        }
        for (namespace, scheme), tables in sorted(drift.items())
    ]


def _unused_in_code(databases, touched):
    """List source tables no scanned job reads or writes.

    Args:
        databases: The catalog ``databases`` list.
        touched: ``id()`` of every table entry a scanned dataset was matched to.

    Returns:
        list[dict]: Sorted ``{"db_name", "table_name"}`` entries.
    """
    return sorted(
        (
            {"db_name": database["db_name"], "table_name": table["table_name"]}
            for database in databases
            for table in database.get("tables", [])
            if id(table) not in touched
        ),
        key=lambda entry: (entry["db_name"], entry["table_name"]),
    )


def _log_drift(schema_drift):
    """Emit one ``schema.drift`` event with counts when a run observer is active.

    Args:
        schema_drift: The ``catalog["schema_drift"]`` block.
    """
    observer = current_observer()
    if observer is None:
        return
    databases = schema_drift["code_only"]["databases"]
    observer.event(
        "schema.drift",
        actor="exporter",
        code_only_tables=sum(len(d["tables"]) for d in databases),
        code_only_columns=sum(len(t["schema"]) for d in databases for t in d["tables"]),
        unused_in_code=len(schema_drift["unused_in_code"]),
    )


def _path_suffix_match(prior_path, source_file):
    """Tell whether two script paths denote the same file up to a directory prefix.

    Args:
        prior_path: ``script_path`` recorded in the prior catalog (e.g. ``etl/job.py``).
        source_file: Path of the scanned job relative to the scan root (e.g. ``job.py``).

    Returns:
        bool: True when one path ends with the other at a ``/`` boundary.
    """
    if not prior_path or not source_file:
        return False
    return prior_path.endswith("/" + source_file) or source_file.endswith("/" + prior_path)


def _match_prior_scripts(prior_scripts, jobs):
    """Pair scanned jobs with prior ``scripts[]`` entries.

    Matching runs in passes so the strongest evidence wins: ``job_id``, then exact
    ``script_path``, then ``script_name``, then a path-suffix match (the catalog was
    built from a different root than the scan). Every prior entry is claimed at most once.

    Args:
        prior_scripts: The prior catalog's ``scripts`` list, in its original order.
        jobs: Scanned :class:`~etl_parser.models.Job` objects.

    Returns:
        tuple[dict[str, dict], list[dict]]: ``(matched, unmatched)`` where ``matched`` maps
        a job id to its prior entry and ``unmatched`` lists the prior entries no job
        claimed, in prior order.
    """
    claimed: set[int] = set()
    matched: dict[str, dict] = {}

    def rule_job_id(job, script):
        return script.get("job_id") == job.id

    def rule_path(job, script):
        return script.get("script_path") == job.source_file

    def rule_name(job, script):
        return script.get("script_name") == job.name

    def rule_suffix(job, script):
        return _path_suffix_match(script.get("script_path"), job.source_file)

    for rule in (rule_job_id, rule_path, rule_name, rule_suffix):
        for job in sorted(jobs, key=lambda j: j.id):
            if job.id in matched:
                continue
            for index, script in enumerate(prior_scripts):
                if index not in claimed and rule(job, script):
                    matched[job.id] = script
                    claimed.add(index)
                    break
    unmatched = [s for i, s in enumerate(prior_scripts) if i not in claimed]
    return matched, unmatched


def export_agent_catalog(doc, prior=None, *, include_code_schema=False):
    """Build (or update) an agent ``catalog.json`` dict from a lineage document.

    The source system (or the prior catalog produced from it) is the truth for
    databases, tables and columns; code only says how data is used. Tables and columns
    seen only in code are therefore reported under ``schema_drift`` and as non-gating
    ``missing_in_source`` items instead of being added to ``databases``, unless
    ``include_code_schema`` is set.

    Args:
        doc: The scanned :class:`~etl_parser.models.LineageDocument`.
        prior: A previously exported catalog dict to merge into. Databases are matched by
            ``db_name`` (see :func:`_find_database`), scripts by job id, path, name or
            path suffix (see :func:`_match_prior_scripts`); anything the current scan did
            not touch is kept unchanged. If omitted, a fresh catalog with empty
            ``databases``/``relations`` is built.
        include_code_schema: When True, code-only tables and columns are added to
            ``databases`` marked ``schema_source: "code"`` (new glue databases get
            ``db_type: "athena"``). They are still listed under ``schema_drift``.

    Returns:
        dict: The catalog, with ``databases`` (schema per table, preserving prior flags
        and descriptions, each description carrying ``description_source``), ``scripts``
        (one entry per job, with ``depends_on`` and the extension field
        ``depends_on_detail``, followed by unmatched prior scripts), ``relations`` (passed
        through from ``prior``), ``relations_inferred`` (from join conditions),
        ``schedules``, ``lineage`` (``column_edges`` and ``unresolved`` including the
        ``missing_in_source`` items) and ``schema_drift`` (``code_only.databases`` in the
        ``databases`` shape plus ``unused_in_code``).
    """
    catalog = copy.deepcopy(prior) if prior is not None else {"databases": [], "relations": []}
    databases = catalog.setdefault("databases", [])
    _mark_human_descriptions(databases)
    prior_scripts, unmatched_scripts = _match_prior_scripts(catalog.get("scripts", []), doc.jobs)

    drift: dict = {}
    missing: list[Unresolved] = []
    touched: set[int] = set()
    for dataset in doc.datasets:
        if dataset.kind != "table" or dataset.id.startswith("frame://"):
            continue
        scheme, namespace, name = split_dataset_id(dataset.id)
        jobs = _referencing_jobs(doc, dataset)
        database = _find_database(databases, namespace, scheme)
        table = None
        if database is not None:
            table = next(
                (t for t in database.setdefault("tables", []) if t["table_name"] == name), None
            )
        created = table is None
        if created:
            _record_code_only(drift, scheme, namespace, name, dataset.columns, jobs)
            missing.append(_missing_in_source(dataset.id, [dataset.id], jobs, whole_table=True))
            if not include_code_schema:
                continue
            if database is None:
                database = {
                    "db_name": namespace,
                    "db_type": _db_type(scheme),
                    "description": "",
                    "tables": [],
                    "schema_source": "code",
                }
                databases.append(database)
            table = {"table_name": name, "description": "", "schema": [], "schema_source": "code"}
            database["tables"].append(table)
        if not database.get("db_type"):
            database["db_type"] = _db_type(scheme)
        if dataset.product:
            database["product"] = dataset.product
        if dataset.layer:
            database["layer"] = dataset.layer
        touched.add(id(table))
        table["dataset_id"] = dataset.id
        existing = {c["field_name"] for c in table.setdefault("schema", [])}
        new_columns = sorted(set(dataset.columns) - existing)
        if new_columns and not created:
            _record_code_only(drift, scheme, namespace, name, new_columns, jobs)
            missing.append(_missing_in_source(dataset.id, new_columns, jobs, whole_table=False))
        if include_code_schema:
            for column in new_columns:
                table["schema"].append(
                    {"field_name": column, "description": "", "schema_source": "code"}
                )

    def reference(ident):
        """Build a ``reads_from``/``writes_to`` entry for a dataset id.

        Args:
            ident: Canonical dataset id such as ``glue://db/table`` or ``s3://bucket/path/``.

        Returns:
            dict: ``{"type": "table" | "s3" | "file", "target": <agent-style name>}``.
        """
        return {
            "type": "s3"
            if ident.startswith("s3://")
            else "file"
            if ident.startswith("file://")
            else "table",
            "target": agent_table_name(ident),
        }

    jobs = {j.id: j for j in doc.jobs}
    scripts = []
    for job in sorted(doc.jobs, key=lambda j: j.id):
        old = prior_scripts.get(job.id, {})
        schedule = doc.schedules.get(job.schedule_id)
        deps = doc.job_dependencies.get(job.id, [])
        entry = {key: value for key, value in old.items() if key != "description_source"}
        scripts.append(
            {
                **entry,
                "script_name": job.name,
                "script_path": job.source_file,
                **_script_description(old, job),
                "language": job.language,
                "schedule": schedule.interval_text if schedule else None,
                "reads_from": [reference(d) for d in sorted(job.inputs)],
                "writes_to": [reference(d) for d in sorted(job.outputs)],
                "depends_on": [jobs[d.job_id].name for d in deps if d.job_id in jobs],
                "depends_on_detail": [d.model_dump(mode="json") for d in deps],
                "job_id": job.id,
            }
        )
    catalog["scripts"] = scripts + unmatched_scripts
    catalog.setdefault("relations", [])
    catalog["relations_inferred"] = _relations_inferred(doc.join_conditions)
    catalog["schedules"] = {k: v.model_dump(mode="json") for k, v in sorted(doc.schedules.items())}
    missing.sort(key=lambda u: (u.source_file or "", u.line or 0, u.kind, u.reason))
    catalog["lineage"] = {
        "column_edges": [e.model_dump(mode="json") for e in doc.column_edges],
        "unresolved": [u.model_dump(mode="json") for u in [*doc.unresolved, *missing]],
    }
    catalog["schema_drift"] = {
        "code_only": {"databases": _drift_databases(drift)},
        "unused_in_code": _unused_in_code(databases, touched),
    }
    _log_drift(catalog["schema_drift"])
    return catalog
