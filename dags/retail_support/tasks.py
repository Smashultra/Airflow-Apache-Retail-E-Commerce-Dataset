"""Readiness, dates and publication for the retail DAG (no PySpark imports)."""

import json
import logging
from datetime import timedelta

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import get_current_context
from airflow.sdk.exceptions import AirflowFailException

from retail_contracts import (
    build_context,
    context_file,
    data_root,
    iso_date,
    json_hash,
    load_context,
    publish_completion,
    token,
)
from retail_support.assets import (
    COMPLETION_ASSETS,
    RFM_CUSTOMERS_ASSET,
    FLAGGED_ANOMALIES_ASSET,
)
from retail_support.publication import (
    rfm_event_metadata,
    rfm_publication,
    branch_publication,
    branch_publication_path,
    branch_event_metadata,
)
from retail_support.spark_lineage import LineageSparkSubmitOperator

LOGGER = logging.getLogger(__name__)


def business_date(context):
    """Require a manual date or use the scheduled/backfill interval."""
    run_type = context["dag_run"].run_type
    run_type = getattr(run_type, "value", run_type)
    override = context["params"].get("business_date")
    if run_type == "manual":
        if override is None:
            raise ValueError("Manual runs require business_date (YYYY-MM-DD)")
        return iso_date(override).isoformat()
    start, end = context["data_interval_start"], context["data_interval_end"]
    if start is None or end is None:
        raise ValueError("A scheduled run requires a data interval")
    if end - start != timedelta(days=1) or any(
        (start.hour, start.minute, start.second, start.microsecond)
    ):
        raise ValueError("Expected a midnight-to-midnight daily interval")
    day = start.date().isoformat()
    if override is not None and override != day:
        raise ValueError("business_date cannot override a scheduled interval")
    return day


class LandingValidationSensor(FileSensor):
    """Wait for a committed manifest, then fail fast on bad file contracts."""

    def execute(self, context):
        try:
            day = business_date(context)
            version = token(context["params"]["source_version"])
        except (ValueError, TypeError, KeyError) as error:
            raise AirflowFailException(str(error)) from error
        self.filepath = f"{version}/manifest.json"
        self.__dict__.pop("path", None)
        # Never swallow the sensor's reschedule/timeout exceptions.
        super().execute(context)
        try:
            return build_context(
                data_root(),
                context["dag"].dag_id,
                context["run_id"],
                day,
                context["params"],
            )
        except (ValueError, OSError, KeyError) as error:
            raise AirflowFailException(str(error)) from error


class BranchSparkSubmitOperator(LineageSparkSubmitOperator):
    """Validate Spark output before task success emits a branch Asset.

    This gate is inside execute, not a best-effort success callback. A failed
    gate fails the task and cannot emit a ready Asset. Retry after publication
    reuses the frozen snapshot without starting another Spark generation.
    """

    stage = None
    asset = None

    def execute(self, context):
        metadata = context["ti"].xcom_pull(task_ids="validate_raw_data")
        root = data_root()
        inputs = load_context(metadata["context_path"])
        if (inputs["dag_id"], inputs["run_id"]) != (
            self.dag_id,
            context["run_id"],
        ):
            raise ValueError("Branch context does not match task run")
        if not branch_publication_path(root, inputs, self.stage).exists():
            super().execute(context)
        summary = branch_publication(root, inputs, self.stage, publish=True)
        extra = branch_event_metadata(root, summary, self.stage)
        if self.asset in self.outlets:
            context["outlet_events"][self.asset].extra = extra
        LOGGER.info(
            "BRANCH_PUBLISHED %s %s",
            self.stage,
            json.dumps(extra, sort_keys=True),
        )


class RfmSparkSubmitOperator(BranchSparkSubmitOperator):
    stage = "rfm"
    asset = RFM_CUSTOMERS_ASSET


class AuditSparkSubmitOperator(BranchSparkSubmitOperator):
    stage = "audit"
    asset = FLAGGED_ANOMALIES_ASSET


def notify_completion(emit_assets=True):
    """Publish only after both branches produced consistent stage manifests."""
    context = get_current_context()
    metadata = context["ti"].xcom_pull(task_ids="validate_raw_data")
    inputs = load_context(metadata["context_path"])
    # Aggregate publication must match both advertised branch snapshots.
    rfm = rfm_publication(data_root(), inputs)
    audit = branch_publication(data_root(), inputs, "audit")
    summary = publish_completion(data_root(), inputs)
    # PythonOperator uses outlet_events, not a generator yielding Metadata.
    # Populate only after validation and atomic publication both succeed.
    extra = {
        **rfm_event_metadata(data_root(), rfm),
        "publication_version": 3,
        "publication_kind": "snapshot",
        "audit_hash": json_hash(audit["stages"]["audit"]),
        "audit_manifest": str(
            branch_publication_path(data_root(), inputs, "audit")
        ),
        "completion_hash": json_hash(summary),
        "published_pointer": str(
            data_root()
            / "manifests"
            / "published"
            / f"{inputs['run_date']}.json"
        ),
        # A later publication can replace the date pointer. Consumers must
        # resolve this run-specific summary to retain the event's identity.
        "completion_manifest": str(
            context_file(data_root(), inputs["run_key"]).with_name(
                "completion.json"
            )
        ),
    }
    if emit_assets:
        for asset in COMPLETION_ASSETS:
            context["outlet_events"][asset].extra = dict(extra)
    LOGGER.info("PIPELINE_COMPLETE %s", json.dumps(summary, sort_keys=True))
    return extra


def training_dates(context):
    """Monthly runs use interval end; manual runs require a cutoff."""
    run_type = context["dag_run"].run_type
    run_type = getattr(run_type, "value", run_type)
    override = context["params"].get("training_cutoff")
    if run_type == "manual":
        if override is None:
            raise ValueError("Manual training requires training_cutoff")
        cutoff = iso_date(override)
    else:
        start, end = (
            context["data_interval_start"],
            context["data_interval_end"],
        )
        if (
            start is None
            or end is None
            or start.day != 1
            or end.day != 1
            or any(
                (
                    start.hour,
                    start.minute,
                    start.second,
                    start.microsecond,
                    end.hour,
                    end.minute,
                    end.second,
                    end.microsecond,
                )
            )
            or end != start.add(months=1)
        ):
            raise ValueError("Expected a calendar-month training interval")
        cutoff = end.date()
        if override is not None and override != cutoff.isoformat():
            raise ValueError("training_cutoff cannot override interval")
    effective = context["params"].get("effective_from")
    return cutoff.isoformat(), effective or (
        cutoff + timedelta(days=1)
    ).isoformat()


def validate_training_input():
    from retail_models import build_training_context

    context = get_current_context()
    try:
        cutoff, effective = training_dates(context)
        return build_training_context(
            data_root(),
            context["dag"].dag_id,
            context["run_id"],
            cutoff,
            {**context["params"], "effective_from": effective},
        )
    except (ValueError, OSError, KeyError) as error:
        raise AirflowFailException(str(error)) from error


def publish_trained_model():
    from retail_models import publish_model

    context = get_current_context()
    metadata = context["ti"].xcom_pull(task_ids="validate_training_input")
    inputs = load_context(metadata["context_path"])
    activation = publish_model(data_root(), inputs)
    LOGGER.info("MODEL_ACTIVATED %s", json.dumps(activation, sort_keys=True))


def log_failure(context):
    ti = context.get("task_instance")
    LOGGER.error(
        "TASK_FAILED dag=%s task=%s run=%s try=%s",
        getattr(ti, "dag_id", None),
        getattr(ti, "task_id", None),
        context.get("run_id"),
        getattr(ti, "try_number", None),
    )


def log_retry(context):
    LOGGER.warning("TASK_RETRY run=%s", context.get("run_id"))


def log_deadline_miss(**kwargs):
    """Handle the limited deadline API context, not normal task context."""
    context = kwargs.get("context", {})
    run = context.get("dag_run", {})
    deadline = context.get("deadline", {})
    if not isinstance(run, dict):
        run = {
            "dag_id": getattr(run, "dag_id", None),
            "run_id": getattr(run, "run_id", None),
        }
    LOGGER.error(
        "DEADLINE_MISSED dag=%s run=%s deadline=%s",
        run.get("dag_id"),
        run.get("run_id"),
        str(deadline),
    )
