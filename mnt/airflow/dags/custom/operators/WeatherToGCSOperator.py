import json
import os
import logging
import tempfile
import pandas as pd

from datetime import timedelta
from custom.hooks.WeatherHook import WeatherHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class WeatherToGCSOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        w_conn_id,
        gcp_conn_id,
        bucket_name,
        bucket_weather_subdir,
        *args,
        **kwargs,
    ):
        super(WeatherToGCSOperator, self).__init__(**kwargs)
        self._conn_id = w_conn_id
        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._bucket_weather_subdir = bucket_weather_subdir

    def execute(self, context):
        weather_hook = WeatherHook(conn_id=self._conn_id, gcs_conn_id=self._gcp_conn_id)
        gcs_hook = GCSHook(self._gcp_conn_id)
        try:
            locations_weather = pd.DataFrame(
                list(weather_hook.get_weather_all_locations())
            )
            locations_weather = self.formating_weather_data(
                locations_weather[
                    pd.to_datetime(locations_weather.dt_txt).dt.strftime("%Y-%m-%d")
                    == context["data_interval_end"].strftime("%Y-%m-%d")
                ]
            )
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = os.path.join(tmp_dir, "weather.json")
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(
                        locations_weather.to_dict("records"),
                        f,
                        ensure_ascii=False,
                    )
                gcs_hook.upload(
                    bucket_name=self._bucket_name,
                    object_name=f"{self._bucket_weather_subdir}/weather_%s.json"
                    % context["data_interval_end"].strftime("%Y-%m-%d"),
                    filename=tmp_path,
                )
        finally:
            weather_hook.close()

    def formating_weather_data(self, df):
        return pd.DataFrame(
            [
                {"dest_id": dest_id, "weather": df_dest_id.to_dict("records")}
                for dest_id, df_dest_id in df[
                    ["dest_id", "main", "visibility", "wind", "dt_txt"]
                ].groupby(["dest_id"])
            ]
        )
