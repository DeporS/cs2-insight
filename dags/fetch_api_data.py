from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import logging

from get_skins_data import save_data_json


def fetch_api_data():
    logging.info("Starting data fetch from API...")
    save_data_json() # Downloads data and saves it to json
    logging.info("Data saved to JSON")