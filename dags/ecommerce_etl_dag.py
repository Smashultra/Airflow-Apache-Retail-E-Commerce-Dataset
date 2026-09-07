from datetime import datetime, timedelta
from airflow import DAG 
from airflow.providers.standard.operators.empty import EmptyOperator 

# default configuration apply to tasks in DAG
default_args = {
    "owner": "airflow", # owner của task/DAG, metadata để biết task thuộc về ai or team nào
    "retries": 1, # nếu 1 task bị fail, Airflow sẽ thử chạy lại 1 lần. Nếu lần retry đó vẫn fail thì task mới được đánh dấu là failed
    "retry_delay": timedelta(minutes=5) # đợi 5min trước khi retry
}

with DAG(
    dag_id = "ecommerce_etl_dag",
    default_args = default_args,
    description = "Daily e-commerce ETL pipeline skeleton",
    schedule = "@daily",
    start_date = datetime(2026, 9, 1),
    catchup = False,
    tags = ["ecommerce", "etl"]
) as dag:
    start_pipeline = EmptyOperator(
        task_id="start_pipeline"
    )
    
    validate_raw_data = EmptyOperator(
        task_id="validate_raw_data"
    )
    
    submit_pyspark_etl = EmptyOperator(
        task_id="submit_pyspark_etl"
    )
    
    compute_rfm = EmptyOperator(
        task_id="compute_rfm"
    )
    
    detect_anomal = EmptyOperator(
        task_id="detect_anomal"
    )
    
    notify_completion = EmptyOperator(
        task_id="notify_completion"
    )

start_pipeline >> validate_raw_data >> submit_pyspark_etl
submit_pyspark_etl >> [compute_rfm, detect_anomal]
[compute_rfm, detect_anomal] >> notify_completion