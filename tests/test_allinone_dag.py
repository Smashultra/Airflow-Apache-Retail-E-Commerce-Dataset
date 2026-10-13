"""Direct dependencies, no Asset emission and same-run consumer validation."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.serialization.serialized_objects import DagSerialization

from ecommerce_etl_dag_allinone import dag
from retail_contracts import atomic_json, context_file, json_hash, stage_file
from retail_support.allinone import run_dashboard, run_marketing
from retail_support.lineage import completed_lineage
from retail_support.tasks import notify_completion


@pytest.fixture
def ready(analytics_ready):
    root, inputs, stages = analytics_ready
    inputs["dag_id"] = dag.dag_id
    inputs["run_key"] = json_hash([dag.dag_id, inputs["run_id"]])
    path = context_file(root, inputs["run_key"])
    atomic_json(path, inputs)
    for name in ("etl", "rfm", "audit"):
        stages[name]["context_hash"] = json_hash(inputs)
        if name != "etl":
            stages[name]["etl_hash"] = json_hash(stages["etl"])
        atomic_json(stage_file(root, inputs, name), stages[name])
    ti = Mock(dag_id=dag.dag_id)
    values = {"validate_raw_data": {"context_path": str(path)}}
    ti.xcom_pull.side_effect = lambda task_ids: values.get(task_ids)
    # Deliberately no outlet_events: the all-in-one run must never access it.
    runtime = {"ti": ti, "run_id": inputs["run_id"]}
    return root, inputs, stages, values, runtime


def test_topology_without_assets():
    assert len(dag.tasks) == 8
    assert all(not task.inlets and not task.outlets for task in dag.tasks)
    assert dag.get_task("execute_voucher_campaign").upstream_task_ids == {
        "compute_rfm_metrics"
    }
    assert dag.get_task("generate_executive_summary").upstream_task_ids == {
        "notify_completion"
    }
    assert dag.get_task("notify_completion").upstream_task_ids == {
        "compute_rfm_metrics",
        "detect_anomalies",
    }
    assert {task.task_id for task in dag.leaves} == {
        "execute_voucher_campaign",
        "generate_executive_summary",
    }
    assert all(task.trigger_rule == "all_success" for task in dag.tasks)
    assert dag.get_task("notify_completion").op_kwargs == {
        "emit_assets": False
    }
    assert dag.get_task("notify_completion").do_xcom_push
    DagSerialization.serialize_dag(dag)


def test_early_marketing_then_aggregate_without_events(ready, caplog):
    root, inputs, stages, values, runtime = ready
    stage_file(root, inputs, "audit").unlink()
    with (
        patch.object(SparkSubmitOperator, "execute") as spark,
        patch(
            "retail_support.tasks.get_current_context", return_value=runtime
        ),
        patch(
            "retail_support.allinone.get_current_context", return_value=runtime
        ),
        caplog.at_level("INFO"),
    ):
        dag.get_task("compute_rfm_metrics").execute(runtime)
        dag.get_task("compute_rfm_metrics").execute(runtime)
        spark.assert_called_once()  # Retry reuses the frozen marker.
        run_marketing()  # No Audit or completion yet.
        assert "MARKETING_DEMO" in caplog.text
        with pytest.raises((FileNotFoundError, ValueError)):
            notify_completion(emit_assets=False)
        with pytest.raises(ValueError, match="completion"):
            run_dashboard()
        atomic_json(stage_file(root, inputs, "audit"), stages["audit"])
        dag.get_task("detect_anomalies").execute(runtime)
        values["notify_completion"] = notify_completion(emit_assets=False)
        # Consumer never follows the replaceable date pointer.
        atomic_json(
            root / "manifests/published/2011-12-10.json", {"bad": True}
        )
        run_dashboard()
        assert "EXECUTIVE_DEMO" in caplog.text
    lineage = completed_lineage(
        SimpleNamespace(
            dag_id=dag.dag_id,
            run_id=inputs["run_id"],
            task_id="notify_completion",
        )
    )
    assert len(lineage.outputs) == 2
    for task_id in ("execute_voucher_campaign", "generate_executive_summary"):
        lineage = completed_lineage(
            SimpleNamespace(
                dag_id=dag.dag_id, run_id=inputs["run_id"], task_id=task_id
            )
        )
        assert len(lineage.inputs) == 1 and not lineage.outputs


def test_consumers_reject_another_run(ready):
    _, _, _, values, runtime = ready
    runtime["run_id"] = "different-run"
    values["notify_completion"] = {
        "source_dag_id": dag.dag_id,
        "source_run_id": "original-run",
    }
    with patch(
        "retail_support.allinone.get_current_context", return_value=runtime
    ):
        with pytest.raises(ValueError, match="different run"):
            run_marketing()
        with pytest.raises(ValueError, match="this DagRun"):
            run_dashboard()


def test_branch_failure_cannot_create_completion(ready):
    root, inputs, _, _, runtime = ready
    with patch.object(
        SparkSubmitOperator, "execute", side_effect=RuntimeError
    ):
        with pytest.raises(RuntimeError):
            dag.get_task("compute_rfm_metrics").execute(runtime)
    assert (
        not context_file(root, inputs["run_key"])
        .with_name("completion.json")
        .exists()
    )
