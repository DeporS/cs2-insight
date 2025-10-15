from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from calculate_daily_avg import calculate_daily_avg_task

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cs2_daily",
    default_args=default_args,
    start_date=datetime(2025, 10, 14, 23, 40),
    schedule_interval="@daily",  # every 1 day
    catchup=False,
    max_active_runs=1,
    tags=["api", "data"],
) as dag:

    daily_avg = PythonOperator(
        task_id="calculate_daily_avg",
        python_callable=calculate_daily_avg_task,
    )

    daily_avg