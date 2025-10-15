from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import logging

from db_init import load_data_to_postgres

def load_to_postgres():
    logging.info("Inserting data into Postgres...")
    load_data_to_postgres() # Inserts data from json to postgres db
    logging.info("Data successfully inserted into Postgres.")