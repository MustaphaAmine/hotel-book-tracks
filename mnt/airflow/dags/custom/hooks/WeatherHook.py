import requests
from airflow.hooks.base_hook import BaseHook

class WeatherHook(BaseHook):
    DEFAULT_SCHEMA = "https"

    def __init__(
        self, conn_id, retry=3
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
        raise NotImplementedError()

    def _get_weather(self):
        raise NotImplementedError()