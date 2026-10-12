"""Asset-driven marketing demo; no external voucher/email dispatch."""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from retail_support.assets import RFM_CUSTOMERS_ASSET
from retail_support.consumer_events import publication_events

LOGGER = logging.getLogger(__name__)


def run_marketing_campaign(triggering_asset_events=None):
    """Log every published RFM snapshot available for a future campaign."""
    for publication in publication_events(
        triggering_asset_events, [RFM_CUSTOMERS_ASSET]
    ):
        LOGGER.info(
            "MARKETING_DEMO segment=high_value_active publication=%s; "
            "voucher dispatch is not configured",
            publication,
        )


with DAG(
    dag_id="retail_marketing_consumer_dag",
    description="Marketing demo triggered by published RFM snapshots",
    schedule=[RFM_CUSTOMERS_ASSET],
    start_date=pendulum.datetime(2010, 12, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "owner": "marketing-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "consumer", "marketing", "asset-driven"],
) as dag:
    execute_campaign = PythonOperator(
        task_id="execute_voucher_campaign",
        python_callable=run_marketing_campaign,
        execution_timeout=timedelta(minutes=2),
        do_xcom_push=False,
    )
