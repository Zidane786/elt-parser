"""OpenLineage events constructed with the official Python models."""

from uuid import NAMESPACE_URL, uuid5

from openlineage.client.facet_v2 import column_lineage_dataset as cl
from openlineage.client.facet_v2 import schema_dataset as schema
from openlineage.client.run import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.serde import Serde

from etl_parser.identity import dataset_ref_from_id

PRODUCER = "https://github.com/Zidane786/elt-parser"


def export_openlineage(doc):
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
                        for ref in edge.sources + edge.indirect_sources:
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
                                            type="DIRECT" if ref in edge.sources else "INDIRECT",
                                            subtype={
                                                "identity": "IDENTITY",
                                                "aggregation": "AGGREGATION",
                                            }.get(edge.transformation.kind, "TRANSFORMATION")
                                            if ref in edge.sources
                                            else None,
                                            description=edge.transformation.expression,
                                        )
                                    ],
                                )
                            )
                        prior = fields.get(edge.target.name)
                        if prior:
                            refs = prior.inputFields + refs
                        refs = list({(r.namespace, r.name, r.field): r for r in refs}.values())
                        fields[edge.target.name] = cl.Fields(
                            inputFields=refs,
                            transformationDescription=edge.transformation.expression,
                            transformationType=edge.transformation.kind.upper(),
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
