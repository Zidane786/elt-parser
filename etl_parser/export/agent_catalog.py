"""Catalog export preserving prior human metadata and flags (spec sections 3.1, 10).

Implements the ``AgentCatalogExporter`` role: writes ``catalog.json`` in the exact shape
``de_agent`` expects (top-level ``databases``, ``relations``, ``scripts``), merging in a
prior catalog when supplied so human/AI descriptions and catalog flags (``to_tokenize``,
``verify``, etc., which are not derivable from code) survive re-scans. Also writes the
``schedules`` and ``lineage`` extension fields the current agent ignores.
"""

import copy
import json
from pathlib import Path

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


def _find_database(databases, namespace, scheme, rival_schemes=()):
    """Pick the catalog database a dataset belongs to, matching by name first.

    A database whose type maps to the dataset's scheme wins, then one with no type. A
    single database with the dataset's name is otherwise the target regardless of its
    ``db_type``, because the source system (which may catalogue a Glue database as
    ``sqlite``), not the code, decides the engine. That last fallback is withheld when
    another dataset in the same namespace does have the prior entry's scheme: the entry
    describes that dataset's database, so this one needs its own.

    Args:
        databases: The catalog ``databases`` list.
        namespace: Namespace part of the dataset id.
        scheme: Scheme part of the dataset id.
        rival_schemes: Schemes of every dataset sharing this namespace, used to withhold
            the single-name fallback.

    Returns:
        dict | None: The matching database entry, or ``None`` when a new one is needed.
    """
    candidates = [d for d in databases if _same_db_name(d.get("db_name"), namespace, scheme)]
    for wanted in (scheme, ""):
        for database in candidates:
            if _database_scheme(database) == wanted:
                return database
    if len(candidates) == 1 and _database_scheme(candidates[0]) not in set(rival_schemes):
        return candidates[0]
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


CATALOG_SECTIONS = ("databases", "scripts", "relations", "lineage", "schedules")
"""Top-level catalog sections ``export_agent_catalog(generate=...)`` can (re)generate.

``schema_drift`` belongs to ``databases`` and ``relations_inferred`` to ``relations``.
"""


def _sections(generate):
    """Validate and normalize the ``generate`` selection.

    Args:
        generate: Iterable of section names, a single name, or ``None``/empty for all.

    Returns:
        set[str]: The selected section names.

    Raises:
        ValueError: If a name is not one of :data:`CATALOG_SECTIONS`.
    """
    if isinstance(generate, str):
        generate = [generate]
    selected = set(generate or ())
    unknown = sorted(selected - set(CATALOG_SECTIONS))
    if unknown:
        raise ValueError(
            f"Unknown catalog section(s) {unknown}; valid sections are {list(CATALOG_SECTIONS)}"
        )
    return selected or set(CATALOG_SECTIONS)


class _Scope:
    """The ``databases`` restriction: which databases the export may (re)generate.

    Glue identifiers are case-insensitive, so glue-scheme datasets and prior databases
    with a Glue-family (or missing) ``db_type`` match names ignoring case; other schemes
    match exactly.

    Attributes:
        names: The selected database names as given.
        lowered: The same names lower-cased.
    """

    def __init__(self, names):
        """Record the selected names.

        Args:
            names: Iterable of ``db_name`` values.
        """
        self.names = set(names)
        self.lowered = {name.lower() for name in self.names}

    def namespace(self, namespace, scheme):
        """Tell whether a dataset namespace is selected.

        Args:
            namespace: Namespace part of a dataset id.
            scheme: Scheme part of a dataset id.

        Returns:
            bool: True when the namespace names a selected database.
        """
        if namespace in self.names:
            return True
        return scheme in {"glue", ""} and namespace.lower() in self.lowered

    def dataset(self, dataset_id):
        """Tell whether a canonical dataset id lies in a selected database.

        Args:
            dataset_id: A ``scheme://namespace/name`` id (anything else is out of scope).

        Returns:
            bool: True when the dataset's namespace is selected.
        """
        if not isinstance(dataset_id, str) or "://" not in dataset_id:
            return False
        scheme, namespace, _ = split_dataset_id(dataset_id)
        return self.namespace(namespace, scheme)

    def table_name(self, agent_name):
        """Tell whether an agent-style ``db.table`` name lies in a selected database.

        Args:
            agent_name: ``db.table`` as written in ``relations`` entries.

        Returns:
            bool: True when the ``db`` part is selected.
        """
        if not isinstance(agent_name, str) or "://" in agent_name:
            return False
        return self.namespace(agent_name.split(".", 1)[0], "")

    def database(self, database):
        """Tell whether a prior catalog database entry is selected.

        The entry's own ``db_type`` is not consulted: the source system may record a
        Glue-catalogued database as ``sqlite``, ``athena`` or anything else, and
        :func:`_find_database` already routes datasets to it by name. Names are therefore
        compared case-insensitively here.

        Args:
            database: A ``databases[]`` dict.

        Returns:
            bool: True when its ``db_name`` is selected.
        """
        name = database.get("db_name") or ""
        return name in self.names or name.lower() in self.lowered

    def job(self, job):
        """Tell whether a job reads or writes any selected database.

        Args:
            job: A :class:`~etl_parser.models.Job`.

        Returns:
            bool: True when at least one input or output dataset is in scope.
        """
        return any(self.dataset(d) for d in [*job.inputs, *job.outputs])


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


def export_agent_catalog(
    doc, prior=None, *, schema=None, include_code_schema=False, generate=None, databases=None
):
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
        generate: Section names from :data:`CATALOG_SECTIONS` to (re)generate; ``None``
            or empty means all. Unselected sections are passed through from ``prior``
            unchanged, or omitted when there is no prior. ``schema_drift`` follows
            ``databases`` and ``relations_inferred`` follows ``relations``.
        databases: ``db_name`` values to restrict generation to; ``None`` means all. Only
            tables, scripts, relations, lineage entries and schedules touching those
            databases are generated; everything else is passed through from ``prior``
            unchanged. Glue-scheme names match case-insensitively.

    Raises:
        ValueError: If ``generate`` names an unknown section.

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
    sections = _sections(generate)
    scope = _Scope(databases) if databases is not None else None
    catalog = copy.deepcopy(prior) if prior is not None else {}
    missing: list[Unresolved] = []
    notes: list[Unresolved] = []
    if "databases" in sections:
        no_source_of_truth = not _seed_from_schema(catalog, schema)
        if no_source_of_truth and not include_code_schema:
            # Nothing authoritative to merge into: fall back to the code-derived schema so
            # the catalog is still usable, and say so instead of returning an empty section.
            include_code_schema = True
            notes.append(
                Unresolved(
                    kind="analysis_note",
                    reason=(
                        "No schema or prior catalog was supplied, so databases, tables and "
                        "columns come from code. Supply a schema or prior catalog for a "
                        "source-of-truth export."
                    ),
                )
            )
        missing = notes + _export_databases(doc, catalog, scope, include_code_schema)
    if "scripts" in sections:
        _export_scripts(doc, catalog, scope)
    if "relations" in sections:
        _export_relations(doc, catalog, scope)
    if "schedules" in sections:
        _export_schedules(doc, catalog, scope)
    if "lineage" in sections:
        _export_lineage(doc, catalog, scope, missing)
    return catalog


def _seed_from_schema(catalog, schema):
    """Seed the catalog's ``databases`` from a source-of-truth schema, if one is supplied.

    The source system is authoritative for databases, tables and columns. A schema may be a
    :class:`~etl_parser.schema.base.SchemaSource`, an already-loaded catalog dict, or a path
    to a catalog JSON file. Entries already present from a prior catalog are kept; schema
    entries only fill in what the prior does not have.

    Args:
        catalog: The catalog being built; its ``databases`` list is extended in place.
        schema: A schema source, catalog dict, or path, or ``None``.

    Returns:
        bool: True when the catalog now has at least one authoritative database entry,
        meaning a prior catalog or schema supplied it; False when there is no source of
        truth and the caller should fall back to the code-derived schema.
    """
    supplied = _schema_databases(schema)
    databases = catalog.setdefault("databases", [])
    known = {d.get("db_name") for d in databases}
    for database in supplied:
        if database.get("db_name") not in known:
            databases.append(copy.deepcopy(database))
    return bool(databases)


def _schema_databases(schema):
    """Return the ``databases`` list from any accepted schema form.

    Args:
        schema: A schema source exposing ``catalog()``, a catalog dict, a path to a catalog
            JSON file, or ``None``.

    Returns:
        list[dict]: The schema's databases, empty when nothing usable was supplied.
    """
    if schema is None:
        return []
    if hasattr(schema, "catalog"):
        try:
            return schema.catalog().get("databases", [])
        except Exception:  # pragma: no cover - a failing source must not break the export.
            return []
    if isinstance(schema, dict):
        return schema.get("databases", [])
    if isinstance(schema, (str, Path)):
        try:
            return json.loads(Path(schema).read_text()).get("databases", [])
        except (OSError, ValueError):
            return []
    return []


def _export_databases(doc, catalog, scope, include_code_schema):
    """Generate the ``databases`` section and ``schema_drift`` (see the exporter docstring).

    Args:
        doc: The lineage document.
        catalog: The catalog being built; ``databases`` and ``schema_drift`` are set on it.
        scope: The ``databases`` restriction, or ``None`` for all.
        include_code_schema: Whether code-only tables and columns are added to ``databases``.

    Returns:
        list[Unresolved]: The ``missing_in_source`` items, sorted, for the lineage section.
    """
    databases = catalog.setdefault("databases", [])
    selected = [d for d in databases if scope is None or scope.database(d)]
    _mark_human_descriptions(selected)
    drift: dict = {}
    missing: list[Unresolved] = []
    touched: set[int] = set()
    schemes: dict[str, set[str]] = {}
    for dataset in doc.datasets:
        if dataset.kind == "table" and not dataset.id.startswith("frame://"):
            dataset_scheme, dataset_namespace, _ = split_dataset_id(dataset.id)
            schemes.setdefault(dataset_namespace, set()).add(dataset_scheme)
    for dataset in doc.datasets:
        if dataset.kind != "table" or dataset.id.startswith("frame://"):
            continue
        scheme, namespace, name = split_dataset_id(dataset.id)
        if scope is not None and not scope.namespace(namespace, scheme):
            continue
        jobs = _referencing_jobs(doc, dataset)
        database = _find_database(databases, namespace, scheme, schemes.get(namespace, set()))
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

    missing.sort(key=lambda u: (u.source_file or "", u.line or 0, u.kind, u.reason))
    catalog["schema_drift"] = {
        "code_only": {"databases": _drift_databases(drift)},
        "unused_in_code": _unused_in_code(selected, touched),
    }
    _log_drift(catalog["schema_drift"])
    return missing


def _export_scripts(doc, catalog, scope):
    """Generate the ``scripts`` section, keeping unmatched prior entries at the end.

    Args:
        doc: The lineage document.
        catalog: The catalog being built; ``scripts`` is set on it.
        scope: The ``databases`` restriction, or ``None`` for all. Jobs touching no
            selected database are left to their prior entries.
    """
    in_scope = [j for j in doc.jobs if scope is None or scope.job(j)]
    prior_scripts, unmatched_scripts = _match_prior_scripts(catalog.get("scripts", []), in_scope)

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
    for job in sorted(in_scope, key=lambda j: j.id):
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


def _export_relations(doc, catalog, scope):
    """Generate ``relations_inferred``; ``relations`` is only passed through.

    Referential relations belong to the source system, so ``relations`` keeps whatever
    the prior catalog (or a schema source, with ``source: "database"``) recorded.

    Args:
        doc: The lineage document.
        catalog: The catalog being built.
        scope: The ``databases`` restriction, or ``None`` for all.
    """
    catalog.setdefault("relations", [])
    inferred = _relations_inferred(doc.join_conditions)
    if scope is not None:
        kept = [
            r
            for r in catalog.get("relations_inferred", [])
            if not (scope.table_name(r.get("from_table")) or scope.table_name(r.get("to_table")))
        ]
        inferred = [
            r
            for r in inferred
            if scope.table_name(r["from_table"]) or scope.table_name(r["to_table"])
        ]
        inferred = sorted(
            inferred + kept,
            key=lambda r: (
                r.get("from_table", ""),
                r.get("from_column", ""),
                r.get("to_table", ""),
                r.get("to_column", ""),
            ),
        )
    catalog["relations_inferred"] = inferred


def _export_schedules(doc, catalog, scope):
    """Generate the ``schedules`` extension dict.

    Args:
        doc: The lineage document.
        catalog: The catalog being built.
        scope: The ``databases`` restriction, or ``None`` for all. Under a restriction only
            schedules of in-scope jobs are regenerated and the rest are passed through.
    """
    schedules = dict(catalog.get("schedules") or {}) if scope is not None else {}
    wanted = {j.schedule_id for j in doc.jobs if scope is None or scope.job(j)}
    for key, schedule in doc.schedules.items():
        if scope is None or key in wanted:
            schedules[key] = schedule.model_dump(mode="json")
    catalog["schedules"] = dict(sorted(schedules.items()))


def _export_lineage(doc, catalog, scope, missing):
    """Generate the ``lineage`` block: column edges and unresolved items.

    Args:
        doc: The lineage document.
        catalog: The catalog being built.
        scope: The ``databases`` restriction, or ``None`` for all. Under a restriction,
            out-of-scope prior entries are kept after the regenerated ones.
        missing: ``missing_in_source`` items from the databases section, already sorted.
    """
    prior_lineage = catalog.get("lineage") or {}
    edges = [e for e in doc.column_edges if scope is None or scope.dataset(e.target.dataset_id)]
    column_edges = [e.model_dump(mode="json") for e in edges]
    items = [*doc.unresolved, *missing]
    kept_unresolved: list[dict] = []
    if scope is not None:
        in_scope_jobs = {j.id for j in doc.jobs if scope.job(j)}
        items = [u for u in items if u.job_id is None or u.job_id in in_scope_jobs]
        column_edges += [
            edge
            for edge in prior_lineage.get("column_edges", [])
            if not scope.dataset((edge.get("target") or {}).get("dataset_id", ""))
        ]
        kept_unresolved = [
            item
            for item in prior_lineage.get("unresolved", [])
            if item.get("job_id") not in in_scope_jobs
        ]
    catalog["lineage"] = {
        "column_edges": column_edges,
        "unresolved": [u.model_dump(mode="json") for u in items] + kept_unresolved,
    }
