"""Readiness, dates and publication for the retail DAG (no PySpark imports)."""

import json
import logging
from datetime import timedelta

from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import get_current_context
from airflow.sdk.exceptions import AirflowFailException

from retail_contracts import (
    build_context,
    data_root,
    iso_date,
    load_context,
    publish_completion,
    token,
)

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


def notify_completion():
    """Publish only after both branches produced consistent stage manifests."""
    context = get_current_context()
    metadata = context["ti"].xcom_pull(task_ids="validate_raw_data")
    inputs = load_context(metadata["context_path"])
    summary = publish_completion(data_root(), inputs)
    LOGGER.info("PIPELINE_COMPLETE %s", json.dumps(summary, sort_keys=True))


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
