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
