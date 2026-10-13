"""Same-DagRun consumers: direct manifests/XCom, no Asset events."""

import logging

from airflow.sdk import get_current_context

from retail_contracts import data_root, load_context
from retail_support.consumer_events import validate_snapshots
from retail_support.publication import rfm_publication, rfm_event_metadata

LOGGER = logging.getLogger(__name__)


def run_marketing():
    context = get_current_context()
    metadata = context["ti"].xcom_pull(task_ids="validate_raw_data")
    inputs = load_context(metadata["context_path"])
    if (inputs["dag_id"], inputs["run_id"]) != (
        context["ti"].dag_id,
        context["run_id"],
    ):
        raise ValueError("Marketing context belongs to a different run")
    publication = rfm_event_metadata(
        data_root(), rfm_publication(data_root(), inputs)
    )
    LOGGER.info(
        "MARKETING_DEMO segment=high_value_active publication=%s; "
        "voucher dispatch is not configured",
        publication,
    )


def run_dashboard():
    context = get_current_context()
    extra = context["ti"].xcom_pull(task_ids="notify_completion")
    if not isinstance(extra, dict) or (
        extra.get("source_dag_id"),
        extra.get("source_run_id"),
    ) != (context["ti"].dag_id, context["run_id"]):
        raise ValueError("Dashboard requires completion from this DagRun")
    publication = validate_snapshots([extra])[0]
    LOGGER.info(
        "EXECUTIVE_DEMO validated aggregate snapshot=%s; "
        "dashboard refresh is not configured",
        publication,
    )
