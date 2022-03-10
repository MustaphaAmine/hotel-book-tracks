import requests

from airflow.hooks.base_hook import BaseHook


class HotelHook(BaseHook):
    DEFAULT_SCHEMA = "https"

    def __init__(
        self, conn_id = "hotel_book_conn", retry=3
    ):
        super().__init__()

        self._retry = retry
        self._conn_id = conn_id

        self._session = None
        self._base_url = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self._session = None
        self._base_url = None

    def get_conn(self):
        config = self.get_connection(self._conn_id)
        schema = config.schema or self.DEFAULT_SCHEMA
        host = config.host
        self._base_url = f"{schema}://{host}/"
        self._session = requests.session()
        return self._session, self._base_url
    
    def get_all_hotels(self):
        raise NotImplementedError()

    def _get_hotel_by_location(self):
        raise NotImplementedError()

    def _get_location_from_gcs(self):
        raise NotImplementedError()