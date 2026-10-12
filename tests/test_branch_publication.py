"""Independent branches and one validated aggregate for Dashboard."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

from ecommerce_etl_dag import dag
from retail_contracts import atomic_json, context_file, stage_file
from retail_support.assets import (
    RFM_CUSTOMERS_ASSET,
    FLAGGED_ANOMALIES_ASSET,
    RETAIL_SNAPSHOT_ASSET,
    PUBLISHED_ASSETS,
)
from retail_support.consumer_events import publication_events, snapshot_events
from retail_support.publication import (
    branch_publication,
    branch_publication_path,
)
from retail_support.tasks import notify_completion

TASKS = {"rfm": "compute_rfm_metrics", "audit": "detect_anomalies"}
ASSETS = {"rfm": RFM_CUSTOMERS_ASSET, "audit": FLAGGED_ANOMALIES_ASSET}


def test_failed_final_publication_emits_no_snapshot(analytics_ready):
    root, context, _ = analytics_ready
    for branch in TASKS:
        branch_publication(root, context, branch, publish=True)
    ctx = runtime(analytics_ready)
    with (
        patch("retail_support.tasks.get_current_context", return_value=ctx),
        patch(
            "retail_support.tasks.publish_completion",
            side_effect=OSError("final storage failure"),
        ),
    ):
        with pytest.raises(OSError):
            notify_completion()
    assert not ctx["outlet_events"][RETAIL_SNAPSHOT_ASSET].extra
    assert all(
        branch_publication_path(root, context, b).is_file() for b in TASKS
    )


def runtime(fixture):
    root, context, _ = fixture
    ti = Mock()
    ti.xcom_pull.return_value = {
        "context_path": str(context_file(root, context["run_key"]))
    }
    return {
        "ti": ti,
        "run_id": context["run_id"],
        "outlet_events": {
            asset: SimpleNamespace(extra={}) for asset in PUBLISHED_ASSETS
        },
    }


@pytest.mark.parametrize("first", ["rfm", "audit"])
def test_each_branch_publishes_with_other_missing_then_dashboard_waits(
    analytics_ready, first
):
    root, context, stages = analytics_ready
    other = "audit" if first == "rfm" else "rfm"
    stage_file(root, context, other).unlink()
    ctx = runtime(analytics_ready)
    with patch.object(SparkSubmitOperator, "execute") as spark:
        dag.get_task(TASKS[first]).execute(ctx)
        original = dict(ctx["outlet_events"][ASSETS[first]].extra)
        assert original and "completion_manifest" not in original
        assert not ctx["outlet_events"][ASSETS[other]].extra
        dag.get_task(TASKS[first]).execute(ctx)
        spark.assert_called_once()  # Retry reuses its marker.
        assert ctx["outlet_events"][ASSETS[first]].extra == original
        with patch(
            "retail_support.tasks.get_current_context", return_value=ctx
        ):
            with pytest.raises((FileNotFoundError, ValueError)):
                notify_completion()
            assert not ctx["outlet_events"][RETAIL_SNAPSHOT_ASSET].extra
            atomic_json(stage_file(root, context, other), stages[other])
            dag.get_task(TASKS[other]).execute(ctx)
            before = {
                a: dict(ctx["outlet_events"][a].extra) for a in ASSETS.values()
            }
            notify_completion()
            assert before == {
                a: ctx["outlet_events"][a].extra for a in ASSETS.values()
            }
    assert publication_events(
        {RFM_CUSTOMERS_ASSET: [ctx["outlet_events"][RFM_CUSTOMERS_ASSET]]},
        [RFM_CUSTOMERS_ASSET],
    )
    event = ctx["outlet_events"][RETAIL_SNAPSHOT_ASSET]
    # A later run replacing the date pointer cannot redirect this consumer.
    atomic_json(
        root / "manifests/published/2011-12-10.json", {"wrong": "latest"}
    )
    assert snapshot_events(
        {RETAIL_SNAPSHOT_ASSET: [event]}, RETAIL_SNAPSHOT_ASSET
    ) == [event.extra]


@pytest.mark.parametrize(
    "damage",
    [
        "etl_hash",
        "context_hash",
        "marker",
        "outputs",
        "incomplete",
        "spark",
        "disk",
    ],
)
def test_audit_failure_never_emits_its_asset(analytics_ready, damage):
    root, context, stages = analytics_ready
    value = stages["audit"]
    if damage in {"etl_hash", "context_hash"}:
        value[damage] = "bad"
    elif damage == "marker":
        (root / value["outputs"]["flagged_orders"] / "_SUCCESS").unlink()
    elif damage == "outputs":
        del value["outputs"]["flagged_orders"]
    elif damage == "incomplete":
        value["complete"] = False
    atomic_json(stage_file(root, context, "audit"), value)
    ctx = runtime(analytics_ready)
    with patch.object(
        SparkSubmitOperator,
        "execute",
        side_effect=RuntimeError("Spark failed")
        if damage == "spark"
        else None,
    ):
        if damage == "disk":
            with patch(
                "retail_support.publication.atomic_json",
                side_effect=OSError("disk failed"),
            ):
                with pytest.raises(OSError):
                    dag.get_task(TASKS["audit"]).execute(ctx)
        else:
            with pytest.raises((ValueError, RuntimeError)):
                dag.get_task(TASKS["audit"]).execute(ctx)
    assert not ctx["outlet_events"][FLAGGED_ANOMALIES_ASSET].extra
    assert not branch_publication_path(root, context, "audit").exists()


@pytest.mark.parametrize("stage", ["rfm", "audit"])
def test_final_rejects_changed_published_branch(analytics_ready, stage):
    root, context, stages = analytics_ready
    for branch in TASKS:
        branch_publication(root, context, branch, publish=True)
    stages[stage]["metrics"] = {"changed": True}
    atomic_json(stage_file(root, context, stage), stages[stage])
    ctx = runtime(analytics_ready)
    with patch("retail_support.tasks.get_current_context", return_value=ctx):
        with pytest.raises(ValueError, match="new DagRun"):
            notify_completion()
    assert not ctx["outlet_events"][RETAIL_SNAPSHOT_ASSET].extra


@pytest.mark.parametrize(
    "damage", ["event_hash", "completion", "marker", "output", "branch_event"]
)
def test_dashboard_rejects_unverified_aggregate(analytics_ready, damage):
    root, context, stages = analytics_ready
    for branch in TASKS:
        branch_publication(root, context, branch, publish=True)
    ctx = runtime(analytics_ready)
    with patch("retail_support.tasks.get_current_context", return_value=ctx):
        notify_completion()
    event = ctx["outlet_events"][RETAIL_SNAPSHOT_ASSET]
    if damage == "event_hash":
        event.extra["completion_hash"] = "bad"
    elif damage == "completion":
        atomic_json(event.extra["completion_manifest"], {"complete": False})
    elif damage == "marker":
        atomic_json(event.extra["audit_manifest"], {"complete": False})
    elif damage == "output":
        (
            root / stages["audit"]["outputs"]["flagged_orders"] / "_SUCCESS"
        ).unlink()
    else:
        event.extra["publication_version"] = 2
    with pytest.raises(ValueError):
        snapshot_events(
            {RETAIL_SNAPSHOT_ASSET: [event]}, RETAIL_SNAPSHOT_ASSET
        )
