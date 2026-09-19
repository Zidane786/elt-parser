"""Catalog export preserving prior human metadata and flags (spec sections 3.1, 10).

Implements the ``AgentCatalogExporter`` role: writes ``catalog.json`` in the exact shape
``de_agent`` expects (top-level ``databases``, ``relations``, ``scripts``), merging in a
prior catalog when supplied so human/AI descriptions and catalog flags (``to_tokenize``,
``verify``, etc., which are not derivable from code) survive re-scans. Also writes the
``schedules`` and ``lineage`` extension fields the current agent ignores.
"""

import copy

from etl_parser.identity import ENGINE_SCHEME, GLUE_ENGINES, agent_table_name, split_dataset_id


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


def export_agent_catalog(doc, prior=None):
    """Build (or update) an agent ``catalog.json`` dict from a lineage document.

    Args:
        doc: The scanned :class:`~etl_parser.models.LineageDocument`.
        prior: A previously exported catalog dict to merge into. Databases, tables, and
            scripts it contains are updated in place (by db/table name, or by job id/source
            path for scripts); anything the current scan did not touch is kept unchanged.
            If omitted, a fresh catalog with empty ``databases``/``relations`` is built.

    Returns:
        dict: The catalog, with ``databases`` (schema per table, preserving unrelated
        prior flags), ``scripts`` (one entry per job, with ``depends_on`` and the
        extension field ``depends_on_detail``), ``schedules``, and ``lineage``
        (``column_edges`` and ``unresolved``) all populated. ``relations`` is passed
        through unchanged from ``prior``, since it is not inferred from code.
    """
    catalog = copy.deepcopy(prior) if prior is not None else {"databases": [], "relations": []}
    databases = catalog.setdefault("databases", [])
    _mark_human_descriptions(databases)
    prior_scripts, unmatched_scripts = _match_prior_scripts(catalog.get("scripts", []), doc.jobs)

    for dataset in doc.datasets:
        if dataset.kind != "table" or dataset.id.startswith("frame://"):
            continue
        scheme, namespace, name = split_dataset_id(dataset.id)
        database = _find_database(databases, namespace, scheme)
        if database is None:
            database = {
                "db_name": namespace,
                "db_type": _db_type(scheme),
                "description": "",
                "tables": [],
            }
            databases.append(database)
        elif not database.get("db_type"):
            database["db_type"] = _db_type(scheme)
        if dataset.product:
            database["product"] = dataset.product
        if dataset.layer:
            database["layer"] = dataset.layer
        table = next(
            (t for t in database.setdefault("tables", []) if t["table_name"] == name), None
        )
        if table is None:
            table = {"table_name": name, "description": "", "schema": []}
            database["tables"].append(table)
        table["dataset_id"] = dataset.id
        existing = {c["field_name"] for c in table.setdefault("schema", [])}
        for column in sorted(set(dataset.columns) - existing):
            table["schema"].append({"field_name": column, "description": ""})

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
    catalog["schedules"] = {k: v.model_dump(mode="json") for k, v in sorted(doc.schedules.items())}
    catalog["lineage"] = {
        "column_edges": [e.model_dump(mode="json") for e in doc.column_edges],
        "unresolved": [u.model_dump(mode="json") for u in doc.unresolved],
    }
    return catalog
