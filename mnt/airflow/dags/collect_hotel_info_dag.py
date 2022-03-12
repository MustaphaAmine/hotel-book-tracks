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
    t1 = BashOperator(
        task_id='print_date',
        bash_command = "date", 
        dag = dag
    )
    t2 = BashOperator(
        task_id='print_ds',
        bash_command = "echo {{ds}}"
    )
    hotels_to_gcs = HotelsToGCSOperator(
        task_id = "upload_hotels_to_gcs",
        dag = dag,
    )
    t3 = BashOperator(
        task_id='print_end',
        bash_command = "echo 'end'", 
        dag = dag
    )
    t1>>hotels_to_gcs>>t2>>t3