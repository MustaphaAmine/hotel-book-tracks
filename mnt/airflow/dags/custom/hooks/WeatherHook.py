import requests
import json
import pandas as pd

from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

class WeatherHook(BaseHook):
    DEFAULT_SCHEMA = "https"

    def __init__(self, conn_id, gcs_conn_id, retry=3):
        super().__init__()
        self._retry = retry
        self._conn_id = conn_id
        self._gcs_conn_id = gcs_conn_id

        self._session = None
        self._base_url = None
        self._headers = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._session = None
        self._base_url = None
        self._headers = None

    def get_conn(self):
        config = self.get_connection(self._conn_id)
        schema = config.schema or self.DEFAULT_SCHEMA
        host = config.host
        password = config.password
        self._base_url = f"{schema}://{host}/"
        self._headers = {"x-rapidapi-host": host, "x-rapidapi-key": password}
        self._session = requests.session()
        return self._session, self._base_url, self._headers

    def get_weather_all_locations(self):
        session, base_url, headers = self.get_conn()
        for location in self._get_location_from_gcs():
            params = {
                "q": f"%s,%s" % (location["city_name"], location["countryCode"])
            }
            yield from self._get_weather_by_location(
                session, base_url, headers, params, location["dest_id"]
            )

    def _get_weather_by_location(
        self, session, base_url, headers, params, dest_id, endpoint="forecast"
    ):
        """
        In this example we're going to be using an endpoint that forcasts weather
        related data for 5 days every 3 hours, we are gonna make use only of the data the first day.
        """
        location_weather_info = pd.DataFrame(
            session.request(
                "GET", base_url + endpoint, headers=headers, params=params
            ).json()["list"]
        )[["main","wind","visibility","dt_txt"]]
        location_weather_info.loc[:, "dest_id"] = dest_id
        yield from location_weather_info.to_dict("records")

    def _get_location_from_gcs(self):
        """
        Downlaod location information from the Google Cloud Storage Bucket
        """
        gcs_hook = GCSHook(self._gcs_conn_id)
        locations_bytes = gcs_hook.download(
            bucket_name="hotel-book-track-bucket",
            object_name="locations/locations.json",
        )
        locations = json.loads(locations_bytes.decode("utf8"))
        return locations
