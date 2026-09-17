"""Catalog export preserving prior human metadata and flags."""

import copy

from etl_parser.identity import ENGINE_SCHEME, GLUE_ENGINES, agent_table_name, split_dataset_id


def export_agent_catalog(doc, prior=None):
    catalog = copy.deepcopy(prior) if prior is not None else {"databases": [], "relations": []}
    databases = catalog.setdefault("databases", [])
    prior_scripts = {s["script_path"]: s for s in catalog.get("scripts", [])}
    prior_jobs = {s["job_id"]: s for s in catalog.get("scripts", []) if s.get("job_id")}

    def database_scheme(database):
        kind = database.get("db_type", "").lower()
        return "glue" if kind in GLUE_ENGINES else ENGINE_SCHEME.get(kind, kind)

    for dataset in doc.datasets:
        if dataset.kind != "table" or dataset.id.startswith("frame://"):
            continue
        scheme, namespace, name = split_dataset_id(dataset.id)
        database = next(
            (
                d
                for d in databases
                if d["db_name"] == namespace and database_scheme(d) in {"", scheme}
            ),
            None,
        )
        if database is None:
            database = {"db_name": namespace, "db_type": scheme, "description": "", "tables": []}
            databases.append(database)
        elif not database.get("db_type"):
            database["db_type"] = scheme
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
