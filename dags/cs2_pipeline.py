from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fetch_api_data import fetch_api_data
from load_to_postgres import load_to_postgres

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cs2_data_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 10, 6),
    schedule_interval="*/30 * * * *",  # every 30 mins
    catchup=False,
    max_active_runs=1,
    tags=["api", "data"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_from_api",
        python_callable=fetch_api_data,
    )

    load_data = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_to_postgres,
    )

    fetch_data >> load_data