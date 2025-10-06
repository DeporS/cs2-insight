from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import logging

from db_init import main as db_main
from get_skins_data import save_data_json


def fetch_api_data():
    logging.info("Starting data fetch from API...")
    save_data_json() # Downloads data and saves it to json
    logging.info("Data saved to JSON. Now loading into Postgres...")
    db_main() # Inserts data from json to postgres db
    logging.info("Data successfully inserted into Postgres.")
    

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="fetch_api_data",
    default_args=default_args,
    start_date=datetime(2025, 10, 6),
    schedule_interval="*/10 * * * *",  # every 10 mins
    catchup=False,
    tags=["api", "data"],
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data_from_api",
        python_callable=fetch_api_data,
    )

    fetch_data