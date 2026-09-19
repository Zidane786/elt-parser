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
    prior_scripts = {s["script_path"]: s for s in catalog.get("scripts", [])}
    prior_jobs = {s["job_id"]: s for s in catalog.get("scripts", []) if s.get("job_id")}

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
        old = prior_jobs.get(job.id, prior_scripts.get(job.source_file, {}))
        schedule = doc.schedules.get(job.schedule_id)
        deps = doc.job_dependencies.get(job.id, [])
        scripts.append(
            {
                **old,
                "script_name": job.name,
                "script_path": job.source_file,
                "description": old.get("description", ""),
                "language": job.language,
                "schedule": schedule.interval_text if schedule else None,
                "reads_from": [reference(d) for d in sorted(job.inputs)],
                "writes_to": [reference(d) for d in sorted(job.outputs)],
                "depends_on": [jobs[d.job_id].name for d in deps if d.job_id in jobs],
                "depends_on_detail": [d.model_dump(mode="json") for d in deps],
                "job_id": job.id,
            }
        )
    catalog["scripts"] = scripts
    catalog["schedules"] = {k: v.model_dump(mode="json") for k, v in sorted(doc.schedules.items())}
    catalog["lineage"] = {
        "column_edges": [e.model_dump(mode="json") for e in doc.column_edges],
        "unresolved": [u.model_dump(mode="json") for u in doc.unresolved],
    }
    return catalog
