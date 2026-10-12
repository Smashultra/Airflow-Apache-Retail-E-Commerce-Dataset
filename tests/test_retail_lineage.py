"""Small JSON fixtures and real provider/file transport; no Spark needed."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import jsonschema
import pytest
from airflow.providers.openlineage.plugins.adapter import OpenLineageAdapter
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import Job, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport

from retail_contracts import atomic_json, context_file, json_hash
from retail_support.lineage import (
    RetailExtractor,
    completed_lineage,
    consumer_lineage,
)


@pytest.fixture
def published(tmp_path, monkeypatch):
    monkeypatch.setenv("RETAIL_DATA_ROOT", str(tmp_path))
    ti = SimpleNamespace(
        dag_id="ecommerce_etl_dag",
        run_id="fixture",
        task_id="notify_completion",
    )
    model = {
        "complete": True,
        "model_version": "v1",
        "k": 2,
        "feature_version": "rfm-log1p-standard-v1",
        "training_cutoff": "2011-12-01",
        "pipeline_path": "models/rfm/version=v1/pipeline",
    }
    model_path = "models/rfm/version=v1/model.json"
    atomic_json(tmp_path / model_path, model)
    context = {
        "dag_id": ti.dag_id,
        "run_id": ti.run_id,
        "run_key": json_hash([ti.dag_id, ti.run_id]),
        "run_date": "2011-12-10",
        "code_version": "fixture-code",
        "source_version": "fixture",
        "source_hash": "fixture-source",
        "model": {
            "model_version": "v1",
            "model_manifest": model_path,
            "model_manifest_hash": json_hash(model),
        },
    }
    path = context_file(tmp_path, context["run_key"])
    atomic_json(path, context)
    stages = {}
    for stage, names in {
        "etl": ["transactions", "audit_input", "parse_errors"],
        "rfm": ["customers", "cluster_profile"],
        "audit": ["flagged_orders", "order_assessments", "row_assessments"],
    }.items():
        outputs = {}
        for name in names:
            physical = f"fixture/{stage}/version=one/{name}"
            folder = tmp_path / physical
            folder.mkdir(parents=True)
            (folder / "_SUCCESS").touch()
            outputs[name] = physical
        manifest = {
            "complete": True,
            "stage": stage,
            "outputs": outputs,
            "context_hash": json_hash(context),
            "metrics": {"rows": 2},
            "etl_hash": json_hash(stages["etl"]) if stages else None,
        }
        if stage == "rfm":
            manifest["model"] = context["model"]
        stages[stage] = manifest
        atomic_json(path.with_name(stage + ".json"), manifest)
    summary = {"context": context, "stages": stages, "complete": True}
    atomic_json(path.with_name("completion.json"), summary)
    atomic_json(
        path.with_name("rfm_publication.json"),
        {
            "context": context,
            "complete": True,
            "stages": {name: stages[name] for name in ("etl", "rfm")},
        },
    )
    atomic_json(
        path.with_name("audit_publication.json"),
        {
            "context": context,
            "complete": True,
            "stages": {name: stages[name] for name in ("etl", "audit")},
        },
    )
    return ti, path, summary


def test_publication_lineage_uses_run_manifest_not_date_pointer(published):
    ti, path, summary = published
    lineage = completed_lineage(ti)
    assert len(lineage.inputs) == 3
    assert len(lineage.outputs) == 2
    assert {d.name.rsplit("/", 1)[-1] for d in lineage.outputs} == {
        "transactions",
        "retail_snapshot",
    }
    assert sum("/staged/" in d.name for d in lineage.inputs) == 1
    assert all("/staged/" not in d.name for d in lineage.outputs)
    metadata = lineage.run_facets["retail_provenance"].metadata
    assert metadata["context_hash"] == json_hash(summary["context"])
    assert metadata["model_version"] == "v1"
    assert metadata["publication_state"] == "published"
    assert metadata["completion_manifest"] == str(
        path.with_name("completion.json")
    )
    ti.task_id = "compute_rfm_metrics"
    rfm = completed_lineage(ti)
    for dag in CONSUMER_DAGS:
        assert {d.name for d in consumer_lineage(dag).inputs} <= {
            d.name for d in lineage.outputs + rfm.outputs
        }


def test_stage_outputs_remain_unpublished(published):
    ti, _, _ = published
    ti.task_id = "submit_pyspark_etl"
    lineage = completed_lineage(ti)
    assert all("/staged/" in d.name for d in lineage.outputs)
    assert (
        lineage.run_facets["retail_provenance"].metadata["publication_state"]
        == "computed"
    )


@pytest.mark.parametrize(
    "task_id", ["notify_completion", "compute_rfm_metrics", "detect_anomalies"]
)
def test_start_failure_and_corrupt_metadata_never_publish(published, task_id):
    ti, path, summary = published
    ti.task_id = task_id
    extractor = RetailExtractor(
        SimpleNamespace(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
        )
    )
    for value in [extractor.extract(), extractor.extract_on_failure(ti)]:
        assert value.inputs  # Suppresses provider's declared-outlet fallback.
        assert not value.outputs
    if task_id == "notify_completion":
        summary["stages"]["audit"]["etl_hash"] = "wrong-generation"
        atomic_json(path.with_name("completion.json"), summary)
    else:
        atomic_json(
            path.with_name(
                "rfm_publication.json"
                if task_id == "compute_rfm_metrics"
                else "audit_publication.json"
            ),
            {"complete": False},
        )
    result = extractor.extract_on_complete(ti)
    assert result.inputs and not result.outputs


def test_real_file_transport_serializes_facets(published, tmp_path):
    ti, _, _ = published
    info = completed_lineage(ti)
    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime="2026-10-11T00:00:00Z",
        run=Run(runId=str(uuid4()), facets=info.run_facets),
        job=Job(namespace="retail-test", name="fixture.notify_completion"),
        inputs=info.inputs,
        outputs=info.outputs,
        producer="https://openlineage.io",
    )
    folder = tmp_path / "events"
    folder.mkdir()
    client = OpenLineageClient(
        transport=FileTransport(FileConfig(str(folder / "event")))
    )
    OpenLineageAdapter(client=client).emit(event)
    paths = list(folder.glob("*"))
    assert len(paths) == 1
    saved = json.loads(paths[0].read_text())
    assert saved["eventType"] == "COMPLETE"
    schema_path = (
        Path(__file__).parents[1]
        / "dags/retail_support/schemas"
        / "retail-lineage-v1.json"
    )
    schema = json.loads(schema_path.read_text())
    jsonschema.validate(
        saved["run"]["facets"]["retail_provenance"],
        schema["$defs"]["RetailRunFacet"],
    )
    jsonschema.validate(
        saved["outputs"][0]["facets"]["retail_snapshot"],
        schema["$defs"]["RetailDatasetFacet"],
    )
    # Provider transport failure must not propagate to the business task.
    failing = Mock()
    failing.transport.kind = "http"
    failing.emit.side_effect = ConnectionError("backend unavailable")
    assert OpenLineageAdapter(client=failing).emit(event) is not None
    failing.emit.assert_called_once()


CONSUMER_DAGS = (
    "retail_marketing_consumer_dag",
    "retail_executive_dashboard_dag",
)


def test_rfm_records_frozen_model_as_input(published):
    ti, path, _ = published
    path.with_name("completion.json").unlink()
    path.with_name("audit.json").unlink()
    ti.task_id = "compute_rfm_metrics"
    result = completed_lineage(ti)
    assert any(
        d.name.endswith("models/rfm/version=v1/pipeline")
        for d in result.inputs
    )
    assert (
        result.run_facets["retail_provenance"].metadata["model_version"]
        == "v1"
    )
    assert (
        result.run_facets["retail_provenance"].metadata["publication_state"]
        == "rfm_published"
    )
    assert any(
        d.name.endswith("analytics/rfm_daily/customers")
        and "/staged/" not in d.name
        for d in result.outputs
    )


def test_training_activation_is_required(published, tmp_path):
    ti, source_path, summary = published
    ti.dag_id = "retail_rfm_training_dag"
    context = dict(
        summary["context"],
        dag_id=ti.dag_id,
        run_key=json_hash([ti.dag_id, ti.run_id]),
        etl_manifest=str(source_path.with_name("etl.json")),
        etl_hash=json_hash(summary["stages"]["etl"]),
    )
    path = context_file(tmp_path, context["run_key"])
    atomic_json(path, context)
    candidate = dict(context["model"], context_hash=json_hash(context))
    atomic_json(path.with_name("training.json"), candidate)
    ti.task_id = "train_rfm_model"
    result = completed_lineage(ti)
    assert result.outputs[0].name.endswith("model.json")
    ti.task_id = "publish_model"
    registry = tmp_path / "manifests/models/registry.json"
    atomic_json(registry, {"activations": []})
    with pytest.raises(ValueError, match="not activated"):
        completed_lineage(ti)
    atomic_json(registry, {"activations": [context["model"]]})
    assert completed_lineage(ti).outputs[0].name.endswith("pipeline")


@pytest.mark.parametrize(
    "task_id", ["notify_completion", "compute_rfm_metrics", "detect_anomalies"]
)
def test_provider_registration_and_outlet_fallback(
    published, monkeypatch, task_id
):
    from airflow.providers.openlineage.extractors.manager import (
        ExtractorManager,
    )
    from airflow.utils.state import TaskInstanceState
    from ecommerce_etl_dag import dag

    monkeypatch.setenv(
        "AIRFLOW__OPENLINEAGE__EXTRACTORS",
        "retail_support.lineage.RetailExtractor",
    )
    ti, _, _ = published
    manager = ExtractorManager()
    ti.task_id = task_id
    task = dag.get_task(task_id)
    result = manager.extract_metadata(
        SimpleNamespace(run_id=ti.run_id),
        task,
        TaskInstanceState.FAILED,
        ti,
    )
    assert result.inputs and not result.outputs


def test_audit_lineage_is_published_without_rfm(published):
    ti, path, _ = published
    path.with_name("rfm.json").unlink()
    path.with_name("rfm_publication.json").unlink()
    path.with_name("completion.json").unlink()
    ti.task_id = "detect_anomalies"
    result = completed_lineage(ti)
    assert any(
        d.name.endswith("audit/anomalies/flagged_orders")
        and "/staged/" not in d.name
        for d in result.outputs
    )
    assert (
        result.run_facets["retail_provenance"].metadata["publication_state"]
        == "audit_published"
    )
