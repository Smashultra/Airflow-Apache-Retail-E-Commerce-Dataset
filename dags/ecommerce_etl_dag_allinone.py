"""Daily ETL and both consumers in one DAG, without Asset scheduling."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import (
    DAG,
    CronDataIntervalTimetable,
    DeadlineAlert,
    DeadlineReference,
    Param,
    SyncCallback,
)

from retail_support.allinone import run_marketing, run_dashboard
from retail_support.spark_lineage import LineageSparkSubmitOperator
from retail_support.tasks import (
    LandingValidationSensor,
    RfmSparkSubmitOperator,
    AuditSparkSubmitOperator,
    log_deadline_miss,
    log_failure,
    log_retry,
    notify_completion,
)

CONTEXT_PATH = (
    "{{ ti.xcom_pull(task_ids='validate_raw_data')['context_path'] }}"
)


def spark_job(
    task_id,
    script,
    minutes,
    *,
    operator=LineageSparkSubmitOperator,
):
    """The pool bounds concurrent JVMs; the Connection sets local[2]."""
    return operator(
        task_id=task_id,
        application=f"/opt/airflow/scripts/{script}",
        application_args=["--pipeline-context", CONTEXT_PATH],
        # Jinja templating 1 cấu trúc cho phép là khi chạy DAG
        # thì mới có giá trị
        conn_id="spark_default",
        deploy_mode="client",
        driver_memory="3g",
        conf={"spark.sql.session.timeZone": "UTC"},
        pool="retail_spark",
        pool_slots=1,
        durable=False,
        do_xcom_push=False,
        execution_timeout=timedelta(minutes=minutes),
    )


with DAG(
    dag_id="ecommerce_etl_dag_allinone",
    description="Retail ETL, RFM, audit, Marketing and Dashboard in one DAG",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=pendulum.datetime(2010, 12, 1, tz="UTC"),
    end_date=pendulum.datetime(2011, 12, 9, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(minutes=90),
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=60),
        callback=SyncCallback(log_deadline_miss),
    ),
    params={
        "business_date": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Manual source day, e.g. 2011-12-09 (cutoff +1 day)",
        ),
        "source_version": Param(
            "online-retail-v1",
            type="string",
            pattern=r"^[A-Za-z0-9_-]{1,80}$",
        ),
        "rfm_k": Param(
            None,
            type=["null", "integer"],
            minimum=2,
            maximum=20,
            description="Optional compatibility check; k comes from model",
        ),
        "model_version": Param(
            None,
            type=["null", "string"],
            pattern=r"^[A-Za-z0-9_-]{1,80}$",
            description="Pin an eligible model, or resolve by cutoff",
        ),
        "churn_days": Param(90, type="integer", minimum=1, maximum=3650),
    },
    default_args={
        "owner": "retail-data-team",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "on_failure_callback": log_failure,
        "on_retry_callback": log_retry,
    },
    tags=["retail", "pyspark", "daily", "all-in-one"],
    doc_md="""
### Retail pipeline
Prepare landing, pool and a trained model first (docs/SETUP.md).
Manual trigger: business_date=2011-12-09, churn_days=90.
Daily only applies the frozen scaler/KMeans; training runs separately.
The exclusive analysis cutoff is 2011-12-10. The daily timetable is bounded
to historical data: use explicit manual runs/backfills, not today's date.
Marketing follows validated RFM; Dashboard follows aggregate completion.
All dependencies are task edges; no Asset schedule, inlets or outlets.
Success requires both consumer tasks as well as the pipeline.
Outputs are versioned; original CSV and legacy Parquet are preserved.
""",
) as dag:
    start_pipeline = EmptyOperator(
        task_id="start_pipeline",
        retries=0,
        execution_timeout=timedelta(minutes=1),
    )
    validate_raw_data = LandingValidationSensor(
        task_id="validate_raw_data",
        filepath="manifest.json",
        fs_conn_id="retail_landing",
        mode="reschedule",
        poke_interval=60,
        timeout=1800,
        soft_fail=False,
        deferrable=False,
        retries=0,
        execution_timeout=timedelta(minutes=2),
        do_xcom_push=True,
    )
    submit_pyspark_etl = spark_job(
        "submit_pyspark_etl", "pyspark_clean.py", 20
    )
    compute_rfm_metrics = spark_job(
        "compute_rfm_metrics",
        "pyspark_rfm.py",
        20,
        operator=RfmSparkSubmitOperator,
    )
    detect_anomalies = spark_job(
        "detect_anomalies",
        "pyspark_anomalies.py",
        30,
        operator=AuditSparkSubmitOperator,
    )
    completion = PythonOperator(
        task_id="notify_completion",
        python_callable=notify_completion,
        op_kwargs={"emit_assets": False},
        trigger_rule="all_success",
        execution_timeout=timedelta(minutes=2),
        do_xcom_push=True,
    )

    start_pipeline >> validate_raw_data >> submit_pyspark_etl
    submit_pyspark_etl >> [compute_rfm_metrics, detect_anomalies]
    [compute_rfm_metrics, detect_anomalies] >> completion

    marketing = PythonOperator(
        task_id="execute_voucher_campaign",
        python_callable=run_marketing,
        execution_timeout=timedelta(minutes=2),
        do_xcom_push=False,
    )
    dashboard = PythonOperator(
        task_id="generate_executive_summary",
        python_callable=run_dashboard,
        execution_timeout=timedelta(minutes=2),
        do_xcom_push=False,
    )
    compute_rfm_metrics >> marketing
    completion >> dashboard
