import json
import simplejson
import os
import logging
import tempfile
import pandas as pd

from datetime import timedelta
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook

class MergeHotelWeather(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        gcp_conn_id,
        bucket_name,
        bucket_weather_subdir,
        bucket_hotel_subdir,
        *args,
        **kwargs,
    ):
        super(MergeHotelWeather, self).__init__(**kwargs)
        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._bucket_hotel_subdir = bucket_hotel_subdir
        self._bucket_weather_subdir = bucket_weather_subdir

    def execute(self, context):
        gcs_hook = GCSHook(self._gcp_conn_id)
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path_hotels = os.path.join(tmp_dir, "hotels.json")
            tmp_path_weather = os.path.join(tmp_dir, "weather.json")
            tmp_writer = os.path.join(tmp_dir, "hotels_weather.json")
            # download the hotels file from the bucket to the temporary directory
            gcs_hook.download(
                bucket_name=self._bucket_name,
                object_name=f"{self._bucket_hotel_subdir}/hotels_%s.json"
                % context["data_interval_end"].strftime("%Y-%m-%d"),
                filename=tmp_path_hotels,
            )
            # download the weather file fromt he bucket to the temporary directory
            gcs_hook.download(
                bucket_name=self._bucket_name,
                object_name=f"{self._bucket_weather_subdir}/weather_%s.json"
                % context["data_interval_end"].strftime("%Y-%m-%d"),
                filename=tmp_path_weather,
            )
            with open(tmp_path_hotels, "r", encoding="utf-8") as f_hotels, open(
                tmp_path_weather, "r", encoding="utf-8"
            ) as f_weather, open(tmp_writer, "w", encoding="utf-8") as f_tmp_writer:
                hotels = pd.DataFrame(json.load(f_hotels))
                weather = pd.DataFrame(json.load(f_weather))
                result = pd.merge(hotels,weather, how="left", on="dest_id")
                # We have to store the data as a Newline-Delimiter JSON (NDJSON) to ingest it into the Bigquery table
                for record in result.to_dict('records'):
                    f_tmp_writer.write(simplejson.dumps(record, ignore_nan=True)+'\n')
                gcs_hook.upload(
                    bucket_name=self._bucket_name,
                    object_name=f"results/hotel_weather.json",
                    filename=tmp_writer,
                )