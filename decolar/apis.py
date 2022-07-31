import requests
import logging
import datetime
import ratelimit

from abc import ABC, abstractmethod
from backoff import expo, on_exception

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DecolarApi(ABC):
    def __init__(
        self,
        h_token: str,
        x_uow_token: str,
        user_agent: str,
        user_id: str,
        tracking_code: str,
        gui_version: str,
    ) -> None:
        self.h_token = h_token
        self.x_uow_token = x_uow_token
        self.user_agent = user_agent
        self.user_id = user_id
        self.tracking_code = tracking_code
        self.gui_version = gui_version
        self.base_endpoint = "https://www.decolar.com/shop/flights-busquets/api/v1"

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
        headers = {
            "User-Agent": self.user_agent,
            "X-UOW": self.x_uow_token,
            "X-Gui-Version": self.gui_version,
            "X-TrackingCode": self.tracking_code,
        }
        logger.info(f"Getting data from endpoint: {endpoint}, params={params}")
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        return response.json()


class WebSearchApi(DecolarApi):
    def _get_endpoint(self, **kwargs) -> str:
        return self.base_endpoint + "/web/search"

    def _get_params(
        self,
        from_airport_iata: str,
        to_airport_iata: str,
        departure_date: datetime.date,
        return_date: datetime.date = None,
        adults: int = 1,
        children: int = 0,
        infants: int = 0,
        offset: int = None,
        search_id: str = None,
        **kwargs,
    ) -> dict:

        if return_date is not None and departure_date > return_date:
            raise RuntimeError("departure_date cannot be greater return_date")

        if from_airport_iata == to_airport_iata:
            raise RuntimeError("origin and destination airports cannot be the same")

        params = {
            "adults": adults,
            "children": children,
            "infants": infants,
            "limit": 10,
            "site": "BR",
            "channel": "site",
            "from": from_airport_iata,
            "to": to_airport_iata,
            "departureDate": departure_date.strftime("%Y-%m-%d"),
            "viewMode": "CLUSTER",
            "h": self.h_token,
            "user": self.user_id,
        }

        if return_date:
            params["returnDate"] = return_date.strftime("%Y-%m-%d")

        if search_id:
            params["searchId"] = search_id

        if offset:
            params["offset"] = offset

        return params
