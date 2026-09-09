from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ecommerce_etl_dag",
    default_args=default_args,
    description="Daily e-commerce ETL pipeline skeleton",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    tags=["ecommerce", "etl"],
) as dag:
    start_pipeline = EmptyOperator(task_id="start_pipeline")
    validate_raw_data = EmptyOperator(task_id="validate_raw_data")
    submit_pyspark_etl = EmptyOperator(task_id="submit_pyspark_etl")
    compute_rfm_metrics = EmptyOperator(task_id="compute_rfm_metrics")
    detect_anomalies = EmptyOperator(task_id="detect_anomalies")
    notify_completion = EmptyOperator(task_id="notify_completion")

    start_pipeline >> validate_raw_data >> submit_pyspark_etl
    submit_pyspark_etl >> [compute_rfm_metrics, detect_anomalies]
    [compute_rfm_metrics, detect_anomalies] >> notify_completion
