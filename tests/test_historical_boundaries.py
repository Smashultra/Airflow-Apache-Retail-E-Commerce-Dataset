"""Manual task eligibility and unchanged final scheduled intervals."""
import pendulum
import pytest
from airflow.timetables.base import DataInterval, TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.serialization.serialized_objects import DagSerialization
from ecommerce_etl_dag import dag as daily
from ecommerce_etl_dag_allinone import dag as allinone
from retail_rfm_training_dag import dag as training
from retail_marketing_consumer_dag import dag as marketing
from retail_executive_dashboard_dag import dag as dashboard


@pytest.mark.parametrize("dag,last_day,period", [
    (daily, "2011-12-09", "days"),
    (allinone, "2011-12-09", "days"),
    (training, "2011-11-01", "months"),
])
def test_final_day_manual_tasks_and_schedule(dag, last_day, period):
    start = pendulum.parse(last_day, tz="UTC")
    assert dag.end_date == start.end_of("day")
    # Same predicate used by installed DagRun.verify_integrity for manual runs.
    for logical_date in (start, start.add(hours=11, minutes=11, seconds=25),
                         start.end_of("day")):
        eligible = [task for task in dag.tasks
                    if (task.start_date is None or task.start_date <= logical_date)
                    and (task.end_date is None or logical_date <= task.end_date)]
        assert len(eligible) == len(dag.tasks)
    assert all(task.end_date < start.add(days=1) for task in dag.tasks)
    assert all(task.end_date < pendulum.datetime(2026, 10, 15, tz="UTC")
               for task in dag.tasks)
    restriction = TimeRestriction(earliest=dag.start_date, latest=dag.end_date,
                                  catchup=True)
    previous = start.subtract(**{period: 1})
    serialized = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))
    assert all(t.end_date == start.end_of("day") for t in serialized.tasks)
    timetable = CronDataIntervalTimetable(dag.timetable.expression,
                                          timezone=dag.timetable.timezone)
    final = timetable.next_dagrun_info(
        last_automated_data_interval=DataInterval(previous, start),
        restriction=restriction,
    )
    assert final.data_interval == DataInterval(start, start.add(**{period: 1}))
    assert timetable.next_dagrun_info(
        last_automated_data_interval=final.data_interval,
        restriction=restriction,
    ) is None


def test_asset_consumers_have_no_expired_end_date():
    for dag in (marketing, dashboard):
        assert dag.end_date is None
        assert all(task.end_date is None for task in dag.tasks)
