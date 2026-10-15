"""Monthly model training, independent of daily scoring completion."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, Param
from retail_support.spark_lineage import LineageSparkSubmitOperator

from retail_support.tasks import (
    log_failure,
    log_retry,
    publish_trained_model,
    validate_training_input,
)

def should_train(params):
    return not params["report_only"]


with DAG(
    dag_id="retail_rfm_training_dag",
    description="Train and activate versioned RFM scaler/KMeans models",
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=pendulum.datetime(2010, 12, 1, tz="UTC"),
    # Include manual logical times throughout the final historical day.
    end_date=pendulum.datetime(2011, 11, 1, tz="UTC").end_of("day"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=90),
    default_args={
        "owner": "retail-data-team",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": log_failure,
        "on_retry_callback": log_retry,
    },
    params={
        "training_cutoff": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Exclusive cutoff; required for manual training",
        ),
        "effective_from": Param(
            None,
            type=["null", "string"],
            format="date",
            description="First scoring cutoff; default training cutoff +1",
        ),
        "source_version": Param(
            "online-retail-v1",
            type="string",
            pattern=r"^[A-Za-z0-9_-]{1,80}$",
        ),
        "etl_manifest": Param(
            None,
            type=["null", "string"],
            description="Optional committed ETL manifest under data root",
        ),
        "rfm_k": Param(2, type="integer", minimum=2, maximum=20,
                       description="K chosen manually; report never overrides it"),
        "report_only": Param(False, type="boolean",
                             description="Only generate K report; skip training and publishing"),
        "k_max": Param(8, type="integer", minimum=2, maximum=20,
                       description="Evaluate K from 2 to this value, subject to available data"),
    },
    tags=["retail", "pyspark", "training", "historical-demo"],
    doc_md="""
Run ETL-only bootstrap first; see docs/SETUP.md. Monthly training consumes
the committed ETL snapshot at interval end, never a daily completion marker.
Manual training requires training_cutoff. All historical activation dates
are simulated. Failed training leaves the serving model unchanged.
Manual runs with an explicit logical date must stay within the historical
start/end dates. For example use 2011-11-01T00:00:00Z for a training run
with training_cutoff=2011-12-01. Logical date and data cutoff are distinct.
Use a different available historical logical date for the report-only run,
e.g. 2011-10-31T00:00:00Z, with the same cutoff and ETL snapshot.

To choose K manually: trigger with report_only=true and training_cutoff.
Read evaluate_k logs and data/analytics/k_selection/<run_key>/k_scores.csv
(optional PNG charts). report.json records the exact etl_manifest and cutoff.
Trigger a NEW run with those same inputs, report_only=false and your rfm_k.
Normal runs generate the report then train with the supplied rfm_k (default 2).
There is no pause for approval and no automatic K selection.
""",
) as dag:
    validate = PythonOperator(
        task_id="validate_training_input",
        python_callable=validate_training_input,
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )
    evaluate_k = LineageSparkSubmitOperator(
        task_id="evaluate_k",
        application="/opt/airflow/scripts/select_kmeans_k.py",
        application_args=[
            "--pipeline-context",
            "{{ ti.xcom_pull(task_ids='validate_training_input')['context_path'] }}",
            "--k-max", "{{ params.k_max }}",
        ],
        conn_id="spark_default",
        deploy_mode="client",
        driver_memory="3g",
        conf={"spark.sql.session.timeZone": "UTC"},
        pool="retail_spark",
        pool_slots=1,
        durable=False,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )
    continue_training = ShortCircuitOperator(
        task_id="continue_training",
        python_callable=should_train,
        ignore_downstream_trigger_rules=True,
    )
    train = LineageSparkSubmitOperator(
        task_id="train_rfm_model",
        application="/opt/airflow/scripts/retail_training.py",
        application_args=[
            "--pipeline-context",
            "{{ ti.xcom_pull(task_ids='validate_training_input')"
            "['context_path'] }}",
        ],
        conn_id="spark_default",
        deploy_mode="client",
        driver_memory="3g",
        conf={"spark.sql.session.timeZone": "UTC"},
        pool="retail_spark",
        pool_slots=1,
        durable=False,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=30),
    )
    publish = PythonOperator(
        task_id="publish_model",
        python_callable=publish_trained_model,
        trigger_rule="all_success",
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=2),
    )
    validate >> evaluate_k >> continue_training >> train >> publish
