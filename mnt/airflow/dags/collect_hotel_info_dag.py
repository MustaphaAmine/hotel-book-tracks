import json
import tempfile
import logging
from os import path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

from custom.operators.HotelsToGCSOperator import HotelsToGCSOperator
from custom.operators.WeatherToGCSOperator import WeatherToGCSOperator

with DAG(
    'bookings_track_dag',
    description = """
        A dag to collect hotel booking data alongside weather 
        and currency exchange rates data to pileup some history
        for later analysis
    """,
    start_date = days_ago(2),
    schedule_interval = '0 0 * * *',
    catchup=True,
) as dag:
    raise NotImplementedError()