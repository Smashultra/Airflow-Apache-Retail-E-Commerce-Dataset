"""Monthly model training, independent of daily scoring completion."""

from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, CronDataIntervalTimetable, Param

from retail_support.tasks import (
    log_failure,
    log_retry,
    publish_trained_model,
    validate_training_input,
)

with DAG(
    dag_id="retail_rfm_training_dag",
    description="Train and activate versioned RFM scaler/KMeans models",
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    start_date=pendulum.datetime(2010, 12, 1, tz="UTC"),
    end_date=pendulum.datetime(2011, 11, 1, tz="UTC"),
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
        "rfm_k": Param(2, type="integer", minimum=2, maximum=20),
    },
    tags=["retail", "pyspark", "training", "historical-demo"],
    doc_md="""
Run ETL-only bootstrap first; see docs/SETUP.md. Monthly training consumes
the committed ETL snapshot at interval end, never a daily completion marker.
Manual training requires training_cutoff. All historical activation dates
are simulated. Failed training leaves the serving model unchanged.
""",
) as dag:
    validate = PythonOperator(
        task_id="validate_training_input",
        python_callable=validate_training_input,
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )
    train = SparkSubmitOperator(
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
    validate >> train >> publish
