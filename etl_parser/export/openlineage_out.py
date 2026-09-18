"""OpenLineage events constructed with the official Python models (spec sections 3.3, 10).

Implements the ``OpenLineageExporter`` role: one static ``RunEvent`` per job, with a
``SchemaDatasetFacet`` on datasets with known columns and a ``ColumnLineageDatasetFacet``
on outputs, so Collibra (via Edge), DataHub, and Marquez can all ingest the same files.
Uses the ``openlineage-python`` generated classes for serialization and validation.
"""

from uuid import NAMESPACE_URL, uuid5

from openlineage.client.facet_v2 import column_lineage_dataset as cl
from openlineage.client.facet_v2 import schema_dataset as schema
from openlineage.client.run import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.serde import Serde

from etl_parser.identity import dataset_ref_from_id

PRODUCER = "https://github.com/Zidane786/elt-parser"


def export_openlineage(doc):
    """Build one OpenLineage ``RunEvent`` dict per job in a lineage document.

    Args:
        doc: The scanned :class:`~etl_parser.models.LineageDocument`.

    Returns:
        list[dict]: One serialized ``RunEvent`` per job, sorted by job id, each with
        ``eventType: COMPLETE``, a run id derived deterministically from the job id and
        scan commit, input/output datasets (with a schema facet when columns are known),
        and a column lineage facet on each output whose columns have edges. Transformation
        kinds map to ``DIRECT`` (subtype ``IDENTITY``/``TRANSFORMATION``/``AGGREGATION``)
        for direct sources and ``INDIRECT`` for indirect sources (filter/join/group-by/
        window). ``eventTime`` uses ``doc.generated_at`` when set, else a static sentinel
        timestamp, since these are static snapshots rather than real execution runs.
    """
    datasets = {d.id: d for d in doc.datasets}
    events = []
    for job in sorted(doc.jobs, key=lambda j: j.id):
        inputs, outputs = [], []
        for ids, target, cls in (
            (job.inputs, inputs, InputDataset),
            (job.outputs, outputs, OutputDataset),
        ):
            for ident in sorted(ids):
                dataset = datasets.get(ident) or dataset_ref_from_id(ident)
                facets = {}
                if dataset.columns:
                    facets["schema"] = schema.SchemaDatasetFacet(
                        fields=[schema.SchemaDatasetFacetFields(name=c) for c in dataset.columns],
                        producer=PRODUCER,
                    )
                if cls is OutputDataset:
                    fields = {}
                    for edge in doc.column_edges:
                        if edge.job_id != job.id or edge.target.dataset_id != ident:
                            continue
                        refs = []
                        roles = [(r, "DIRECT") for r in edge.sources]
                        roles += [(r, "INDIRECT") for r in edge.indirect_sources]
                        for ref, role in roles:
                            source = datasets.get(ref.dataset_id) or dataset_ref_from_id(
                                ref.dataset_id
                            )
                            refs.append(
                                cl.InputField(
                                    namespace=source.namespace,
                                    name=source.name,
                                    field=ref.name,
                                    transformations=[
                                        cl.Transformation(
                                            type=role,
                                            subtype={
                                                "identity": "IDENTITY",
                                                "aggregation": "AGGREGATION",
                                            }.get(edge.transformation.kind, "TRANSFORMATION")
                                            if role == "DIRECT"
                                            else None,
                                            description=edge.transformation.expression,
                                        )
                                    ],
                                )
                            )
                        prior = fields.get(edge.target.name)
                        if prior:
                            refs = prior.inputFields + refs
                        merged = {}
                        for ref in refs:
                            key = (ref.namespace, ref.name, ref.field)
                            if key in merged:
                                transforms = merged[key].transformations + ref.transformations
                                merged[key].transformations = list(
                                    {
                                        (t.type, t.subtype, t.description): t for t in transforms
                                    }.values()
                                )
                            else:
                                merged[key] = ref
                        expressions = [edge.transformation.expression]
                        if prior:
                            expressions.append(prior.transformationDescription)
                        fields[edge.target.name] = cl.Fields(
                            inputFields=list(merged.values()),
                            transformationDescription="\n".join(
                                dict.fromkeys(
                                    expression for expression in expressions if expression
                                )
                            )
                            or None,
                            transformationType=(
                                "MIXED"
                                if prior
                                and prior.transformationType != edge.transformation.kind.upper()
                                else edge.transformation.kind.upper()
                            ),
                        )
                    if fields:
                        facets["columnLineage"] = cl.ColumnLineageDatasetFacet(
                            fields=fields, producer=PRODUCER
                        )
                target.append(cls(namespace=dataset.namespace, name=dataset.name, facets=facets))
        # Static snapshots use a stable sentinel time, not a fabricated execution timestamp.
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=doc.generated_at.isoformat() if doc.generated_at else "1970-01-01T00:00:00Z",
            run=Run(runId=str(uuid5(NAMESPACE_URL, f"{doc.scan_commit or 'static'}:{job.id}"))),
            job=Job(namespace="etl-parser/static", name=job.id),
            producer=PRODUCER,
            inputs=inputs,
            outputs=outputs,
        )
        events.append(Serde.to_dict(event))
    return events
