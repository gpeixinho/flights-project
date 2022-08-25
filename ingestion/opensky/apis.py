import requests
import logging
import datetime
import ratelimit

from abc import ABC, abstractmethod
from backoff import expo, on_exception
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class EmptyResponseException(Exception):
    def __init__(self):
        self.message = f"Got HTTP error 404 and empty response from server"
        super().__init__(self.message)


class OpenSkyApi(ABC):
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password
        self.base_endpoint = "https://opensky-network.org/api"

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        pass

    @abstractmethod
    def _get_params(self, **kwargs) -> dict:
        pass

    @on_exception(expo, ratelimit.exception.RateLimitException, max_tries=10)
    @ratelimit.limits(calls=29, period=30)
    @on_exception(expo, requests.exceptions.HTTPError, max_tries=10)
    def get_data(self, **kwargs) -> dict:
        endpoint = self._get_endpoint(**kwargs)
        params = self._get_params(**kwargs)
        logger.info(f"Getting data from endpoint: {endpoint}, params={params}")
        try:
            response = requests.get(
                endpoint, auth=(self.username, self.password), params=params
            )
            response.raise_for_status()
        except HTTPError as err:
            if err.response.status_code == 404 and len(response.json()) == 0:
                logger.error(f"Got empty response from server")
                raise (EmptyResponseException) from err
            else:
                raise
        return response.json()


class AirportFlightsApi(OpenSkyApi):
    def __init__(self, username: str, password: str, airport: str):
        super().__init__(username, password)
        self.airport = airport

    def _get_unix_epoch(self, datetime: datetime) -> int:
        return int(datetime.timestamp())

    def _get_endpoint(self, type: str, **kwargs) -> str:
        if type not in ["arrival", "departure", "all"]:
            raise ValueError("type should be either arrival or departure")
        endpoint = f"{self.base_endpoint}/flights/{type}"
        return endpoint

    def _get_params(self, begin: datetime, end: datetime, **kwargs) -> dict:
        if begin > end:
            raise RuntimeError("begin cannot be greater end")
        unix_begin = self._get_unix_epoch(begin)
        unix_end = self._get_unix_epoch(end)
        unix_now = self._get_unix_epoch(datetime.datetime.now())
        if unix_end - unix_begin > 604800:
            raise RuntimeError("the time interval cannot be greater than 7 days")
        if unix_begin > unix_now:
            raise RuntimeError("begin date cannot be later than now")
        params = {}
        params["airport"] = self.airport
        params["begin"] = str(self._get_unix_epoch(begin))
        params["end"] = str(self._get_unix_epoch(end))
        return params
