"""Executive reporting demo consuming only validated aggregate snapshots."""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from retail_support.assets import RETAIL_SNAPSHOT_ASSET
from retail_support.consumer_events import snapshot_events

LOGGER = logging.getLogger(__name__)


def build_executive_summary(triggering_asset_events=None):
    """Validate each aggregate snapshot and log the dashboard integration."""
    for publication in snapshot_events(
        triggering_asset_events,
        RETAIL_SNAPSHOT_ASSET,
    ):
        LOGGER.info(
            "EXECUTIVE_DEMO validated aggregate snapshot=%s; "
            "dashboard refresh is not configured",
            publication,
        )


with DAG(
    dag_id="retail_executive_dashboard_dag",
    description="Executive demo triggered by validated aggregate snapshots",
    schedule=[RETAIL_SNAPSHOT_ASSET],
    start_date=pendulum.datetime(2010, 12, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "owner": "executive-analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "consumer", "dashboard", "aggregate-asset"],
) as dag:
    generate_dashboard = PythonOperator(
        task_id="generate_executive_summary",
        python_callable=build_executive_summary,
        execution_timeout=timedelta(minutes=2),
        do_xcom_push=False,
    )
