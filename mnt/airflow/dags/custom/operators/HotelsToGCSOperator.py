import json
import os
import logging
import tempfile

from datetime import timedelta
from custom.hooks.HotelHook import HotelHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class HotelsToGCSOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        h_conn_id="hotel_book_conn",
        gcp_conn_id="google_cloud_default",
        bucket_name="hotel-book-track-bucket",
        bucket_hotel_sub_dir="hotel-bookings",
        *args,
        **kwargs,
    ):
        super(HotelsToGCSOperator, self).__init__(**kwargs)
        self._conn_id = h_conn_id
        self._gcp_conn_id = gcp_conn_id
        self._bucket_name = bucket_name
        self._bucket_hotel_sub_dir = bucket_hotel_sub_dir

    def execute(self, context):
        hotel_hook = HotelHook(conn_id=self._conn_id, gcs_conn_id=self._gcp_conn_id)
        gcs_hook = GCSHook(self._gcp_conn_id)
        try:
            hotels = list(hotel_hook.get_all_hotels(checkin_date=context["data_interval_end"].strftime("%Y-%m-%d"),
                            checkout_date=(context["data_interval_end"] + timedelta(days = 1)).strftime("%Y-%m-%d")))
            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = os.path.join(tmp_dir, "hotels.json")
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(hotels, f, ensure_ascii=False)
                gcs_hook.upload(
                    bucket_name=self._bucket_name,
                    object_name=f"{self._bucket_hotel_sub_dir}/hotels_%s.json"
                    % context["data_interval_end"].strftime("%Y-%m-%d"),
                    filename=tmp_path,
                )
        finally:
            hotel_hook.close()
