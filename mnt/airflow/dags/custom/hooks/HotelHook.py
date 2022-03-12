import requests
import json
import logging
import pandas as pd

from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

class HotelHook(BaseHook):
    DEFAULT_SCHEMA = "https"

    def __init__(self, conn_id="hotel_book_conn", gcs_conn_id="google_cloud_default", retry=3):
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

  
    def get_all_hotels(
        self,
        checkin_date="2022-04-08",
        checkout_date="2022-04-09",
        room_number=str(1),
        locale="en-gb",
        adults_number=str(2),
        units="metric",
    ):
        """
        This method iterates over locations and retrieves the hotels available in each one,
        based on certain criteria which are relative to each case of study,
        here I used only required parameters,
        you can check the full list of params in the docs of the API.
        """
        session, base_url, headers = self.get_conn()
        for location in self._get_location_from_gcs():
            params = {
                "checkin_date": checkin_date,
                "checkout_date": checkout_date,
                "room_number": room_number,
                "filter_by_currency": location["currency"],
                "dest_type": location["dest_type"],
                "locale": locale,
                "adults_number": adults_number,
                "order_by": "popularity",
                "units": units,
                "dest_id": location["dest_id"],
            }
            yield from self._get_hotel_by_location(session, base_url, headers, params)

    def _get_hotel_by_location(
        self, session, base_url, headers, params, endpoint="v1/hotels/search"
    ):
        hotel_relevant_info = [
            "hotel_id",
            "hotel_name",
            "countrycode",
            "city",
            "address",
            "zip",
            "timezone",
            "latitude",
            "longitude",
            "price_breakdown",
            "min_total_price",
            "review_score",
            "review_score_word",
            "url",
        ]
        yield from pd.DataFrame(
            session.request(
                "GET", base_url + endpoint, headers=headers, params=params
            ).json()["result"]
        )[hotel_relevant_info].to_dict("records")

    def _get_location_from_gcs(self):
        """Downlaod location information from the Google Cloud Storage Bucket"""
        gcs_hook = GCSHook(self._gcs_conn_id)
        locations_bytes = gcs_hook.download(
            bucket_name="hotel-book-track-bucket",
            object_name="locations/locations.json",
        )
        locations = json.loads(locations_bytes.decode("utf8"))
        return locations