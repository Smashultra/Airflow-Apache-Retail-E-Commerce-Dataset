"""DAG dates, readiness lifecycle and failure propagation contracts."""

from types import SimpleNamespace
from unittest.mock import patch

import pendulum
import pytest
from airflow.exceptions import AirflowFailException, AirflowRescheduleException
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

from ecommerce_etl_dag import dag
from retail_support.tasks import LandingValidationSensor, business_date
from retail_support.tasks import training_dates
from retail_rfm_training_dag import dag as training_dag
from retail_marketing_consumer_dag import dag as marketing_dag
from retail_executive_dashboard_dag import dag as executive_dag
from retail_support.assets import (
    COMPLETION_ASSETS,
    FLAGGED_ANOMALIES_ASSET,
    RETAIL_SNAPSHOT_ASSET,
    RFM_CUSTOMERS_ASSET,
)


def context(run_type="manual", day="2011-12-09"):
    return {
        "dag_run": SimpleNamespace(run_type=run_type),
        "params": {"business_date": day, "source_version": "fixture"},
        "data_interval_start": pendulum.datetime(2011, 12, 9, tz="UTC"),
        "data_interval_end": pendulum.datetime(2011, 12, 10, tz="UTC"),
        "dag": dag,
        "run_id": "test",
    }


def test_topology_real_operators_and_success_only_leaf():
    assert len(dag.tasks) == 6
    assert isinstance(
        dag.get_task("validate_raw_data"), LandingValidationSensor
    )
    for task_id in (
        "submit_pyspark_etl",
        "compute_rfm_metrics",
        "detect_anomalies",
    ):
        assert isinstance(dag.get_task(task_id), SparkSubmitOperator)
    assert dag.get_task("submit_pyspark_etl").downstream_task_ids == {
        "compute_rfm_metrics",
        "detect_anomalies",
    }
    assert dag.get_task("notify_completion").upstream_task_ids == {
        "compute_rfm_metrics",
        "detect_anomalies",
    }
    assert dag.get_task("notify_completion").trigger_rule == "all_success"
    assert dag.deadline is not None


def test_rfm_emits_early_and_consumers_keep_asset_schedules():
    assert dag.get_task("notify_completion").outlets == list(COMPLETION_ASSETS)
    assert dag.get_task("compute_rfm_metrics").outlets == [RFM_CUSTOMERS_ASSET]
    assert all(
        not task.outlets
        for task in dag.tasks
        if task.task_id
        not in {"notify_completion", "compute_rfm_metrics", "detect_anomalies"}
    )
    assert marketing_dag.schedule == [RFM_CUSTOMERS_ASSET]
    assert dag.get_task("detect_anomalies").outlets == [
        FLAGGED_ANOMALIES_ASSET
    ]
    assert executive_dag.schedule == [RETAIL_SNAPSHOT_ASSET]


@pytest.mark.parametrize("failure", [None, "spark", "manifest", "disk"])
def test_rfm_execute_gates_asset_before_task_success(rfm_ready, failure):
    from unittest.mock import Mock
    from retail_contracts import atomic_json, context_file, stage_file
    from retail_support.publication import rfm_publication_path

    root, inputs, stages = rfm_ready
    outlets = {RFM_CUSTOMERS_ASSET: SimpleNamespace(extra={})}
    ctx = {"ti": Mock(), "outlet_events": outlets, "run_id": inputs["run_id"]}
    ctx["ti"].xcom_pull.return_value = {
        "context_path": str(context_file(root, inputs["run_key"]))
    }
    if failure == "manifest":
        stages["rfm"]["etl_hash"] = "wrong"
        atomic_json(stage_file(root, inputs, "rfm"), stages["rfm"])

    def spark(*args):
        assert not outlets[RFM_CUSTOMERS_ASSET].extra
        if failure == "spark":
            raise RuntimeError("spark failed")

    def disk(*args):
        raise OSError("disk failed")

    with patch.object(
        SparkSubmitOperator, "execute", side_effect=spark
    ) as submit:
        task = dag.get_task("compute_rfm_metrics")
        if failure == "disk":
            with patch(
                "retail_support.publication.atomic_json", side_effect=disk
            ):
                with pytest.raises(OSError, match="disk failed"):
                    task.execute(ctx)
        elif failure:
            with pytest.raises((RuntimeError, ValueError)):
                task.execute(ctx)
        else:
            task.execute(ctx)
            extra = dict(outlets[RFM_CUSTOMERS_ASSET].extra)
            assert extra["rfm_manifest"] == str(
                rfm_publication_path(root, inputs)
            )
            assert "completion_manifest" not in extra
            # Retry reuses the committed generation and event identity.
            task.execute(ctx)
            assert outlets[RFM_CUSTOMERS_ASSET].extra == extra
        submit.assert_called_once()
    if failure:
        assert not outlets[RFM_CUSTOMERS_ASSET].extra
        assert not rfm_publication_path(root, inputs).exists()


def test_manual_requires_date_and_scheduled_forbids_wrong_override():
    assert business_date(context()) == "2011-12-09"
    with pytest.raises(ValueError, match="require"):
        business_date(context(day=None))
    assert business_date(context("scheduled", None)) == "2011-12-09"
    with pytest.raises(ValueError, match="override"):
        business_date(context("backfill", "2011-12-08"))


def test_timetable_last_interval_and_end_boundary():
    start = pendulum.datetime(2011, 12, 9, tz="UTC")
    end = start.add(days=1)
    restriction = TimeRestriction(earliest=start, latest=start, catchup=True)
    # The SDK carries a declaration; scheduling methods live server-side.
    timetable = CronDataIntervalTimetable(
        dag.timetable.expression,
        timezone=dag.timetable.timezone,
    )
    info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=restriction,
    )
    assert info.data_interval == DataInterval(start, end)
    assert (
        timetable.next_dagrun_info(
            last_automated_data_interval=info.data_interval,
            restriction=restriction,
        )
        is None
    )


def test_sensor_reschedule_never_creates_context():
    sensor = dag.get_task("validate_raw_data")
    with (
        patch(
            "airflow.providers.standard.sensors.filesystem.FileSensor.execute",
            side_effect=AirflowRescheduleException(pendulum.now("UTC")),
        ),
        patch("retail_support.tasks.build_context") as build,
    ):
        with pytest.raises(AirflowRescheduleException):
            sensor.execute(context())
        build.assert_not_called()


def test_sensor_ready_validates_and_invalid_source_fails_without_retry():
    sensor = dag.get_task("validate_raw_data")
    with (
        patch(
            "airflow.providers.standard.sensors.filesystem.FileSensor.execute",
            return_value=None,
        ),
        patch(
            "retail_support.tasks.build_context",
            side_effect=ValueError("bad CSV"),
        ),
    ):
        with pytest.raises(AirflowFailException, match="bad CSV"):
            sensor.execute(context())


def test_training_topology_pool_and_monthly_dates():
    assert len(training_dag.tasks) == 3
    train = training_dag.get_task("train_rfm_model")
    assert isinstance(train, SparkSubmitOperator)
    assert train.pool == dag.get_task("compute_rfm_metrics").pool
    assert train.upstream_task_ids == {"validate_training_input"}
    assert train.downstream_task_ids == {"publish_model"}
    assert training_dag.get_task("publish_model").trigger_rule == "all_success"
    ctx = context("scheduled", None)
    ctx["data_interval_start"] = pendulum.datetime(2011, 1, 1, tz="UTC")
    ctx["data_interval_end"] = pendulum.datetime(2011, 2, 1, tz="UTC")
    assert training_dates(ctx) == ("2011-02-01", "2011-02-02")
    ctx["params"]["training_cutoff"] = "2011-02-02"
    with pytest.raises(ValueError, match="override"):
        training_dates(ctx)
    with pytest.raises(ValueError, match="requires"):
        training_dates(context())
    ctx = context()
    ctx["params"]["training_cutoff"] = "2011-12-01"
    assert training_dates(ctx) == ("2011-12-01", "2011-12-02")
