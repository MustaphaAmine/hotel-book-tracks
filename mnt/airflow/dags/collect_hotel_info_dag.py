import json
import tempfile
import logging
from os import path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

from custom.operators.HotelsToGCSOperator import HotelsToGCSOperator
from custom.operators.WeatherToGCSOperator import WeatherToGCSOperator
from custom.operators.MergeHotelWeather import MergeHotelWeather
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
            "owner": "airflow",
            "start_date": days_ago(2),
            "depends_on_past": False,
            "email_on_failure": False,
            "email_on_retry": False,
            "email": "fm_debbih@esi.dz",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

with DAG(
    'bookings_track_dag',
    description = """
        A dag to collect hotel booking data alongside weather 
        and currency exchange rates data to pileup some history
        for later analysis
    """,
    default_args=default_args,
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
        h_conn_id="hotel_book_conn",
        gcp_conn_id="google_cloud_default",
        bucket_name="hotel-book-track-bucket",
        bucket_hotel_subdir="hotel-bookings",
        dag = dag,
    )
    weather_to_gcs = WeatherToGCSOperator(
        task_id = "upload_weather_to_gcs",
        w_conn_id="weather_api_conn",
        gcp_conn_id="google_cloud_default",
        bucket_name="hotel-book-track-bucket",
        bucket_weather_subdir="weather-data",
        dag = dag,
    )
    add_weather_to_hotel_push_to_gcs = MergeHotelWeather(
        task_id = "add_weather_to_hotel_push_to_gcs",
        gcp_conn_id="google_cloud_default",
        bucket_name="hotel-book-track-bucket",
        bucket_weather_subdir="weather-data",
        bucket_hotel_subdir="hotel-bookings",
        dag = dag,
    )
    gcs2bigquery = GCSToBigQueryOperator( 
        task_id="gcs2bigquery",
        bucket="hotel-book-track-bucket",
        source_objects="results/hotel_weather.json",
        destination_project_dataset_table="hotel-book-tracks:HotelBookings.hotel_weather",
        schema_object="schemas/hotels_one_night_schema.json",
        source_format='NEWLINE_DELIMITED_JSON',
        max_bad_records=10,
        bigquery_conn_id="google_cloud_default",
        google_cloud_storage_conn_id="google_cloud_default"
    )
    send_email_notification = EmailOperator(
        task_id="email_notification",
        to="fm_debbih@esi.dz",
        subject="hotel booking scrating data update",
        html_content="""
            <p>
            Hello, <br/> <br/>

            This is an email generated automatically, to notify you that the Apache Airflow DAG 
            that reads hotel booking data and weather data from different APIs and write the 
            results to a BigQuery table has succeded.<br/><br/>

            Best Regards
            </p>
        """
    )
    t3 = BashOperator(
        task_id='print_end',
        bash_command = "echo 'end'",
        dag = dag
    )
