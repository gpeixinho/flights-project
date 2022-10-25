import datetime
import pytest
from ingestion.decolar.apis import WebSearchApi


class TestWebSearchApi:
    def test_get_endpoint(self):
        actual = WebSearchApi(
            h_token="xxx",
            x_uow_token="xxx",
            user_agent="xxx",
            user_id="xxx",
            gui_version="xxx",
            tracking_code="xxx",
        )._get_endpoint()
        expected = "https://www.decolar.com/shop/flights-busquets/api/v1/web/search"
        assert actual == expected

    @pytest.mark.parametrize(
        # "from_airport_iata, to_airport_iata, departure_date, return_date, adults, children, infants, expected",
        "kwargs, expected",
        [
            (
                {
                    "from_airport_iata": "CLT",
                    "to_airport_iata": "TPA",
                    "departure_date": datetime.date(2022, 7, 15),
                    "return_date": datetime.date(2022, 7, 22),
                },
                {
                    "adults": 1,
                    "children": 0,
                    "infants": 0,
                    "limit": 10,
                    "site": "BR",
                    "channel": "site",
                    "from": "CLT",
                    "to": "TPA",
                    "departureDate": "2022-07-15",
                    "returnDate": "2022-07-22",
                    "viewMode": "CLUSTER",
                    "h": "xxx",
                    "user": "xxx",
                },
            ),
            (
                {
                    "from_airport_iata": "CLT",
                    "to_airport_iata": "TPA",
                    "departure_date": datetime.date(2022, 7, 1),
                    "return_date": datetime.date(2022, 7, 10),
                },
                {
                    "adults": 1,
                    "children": 0,
                    "infants": 0,
                    "limit": 10,
                    "site": "BR",
                    "channel": "site",
                    "from": "CLT",
                    "to": "TPA",
                    "departureDate": "2022-07-01",
                    "returnDate": "2022-07-10",
                    "viewMode": "CLUSTER",
                    "h": "xxx",
                    "user": "xxx",
                },
            ),
            (
                {
                    "from_airport_iata": "CLT",
                    "to_airport_iata": "TPA",
                    "departure_date": datetime.date(2022, 7, 1),
                    "return_date": datetime.date(2022, 7, 10),
                    "adults": 2,
                },
                {
                    "adults": 2,
                    "children": 0,
                    "infants": 0,
                    "limit": 10,
                    "site": "BR",
                    "channel": "site",
                    "from": "CLT",
                    "to": "TPA",
                    "departureDate": "2022-07-01",
                    "returnDate": "2022-07-10",
                    "viewMode": "CLUSTER",
                    "h": "xxx",
                    "user": "xxx",
                },
            ),
            (
                {
                    "from_airport_iata": "CLT",
                    "to_airport_iata": "TPA",
                    "departure_date": datetime.date(2022, 7, 1),
                    "return_date": datetime.date(2022, 7, 10),
                    "children": 2,
                },
                {
                    "adults": 1,
                    "children": 2,
                    "infants": 0,
                    "limit": 10,
                    "site": "BR",
                    "channel": "site",
                    "from": "CLT",
                    "to": "TPA",
                    "departureDate": "2022-07-01",
                    "returnDate": "2022-07-10",
                    "viewMode": "CLUSTER",
                    "h": "xxx",
                    "user": "xxx",
                },
            ),
            (
                {
                    "from_airport_iata": "CLT",
                    "to_airport_iata": "TPA",
                    "departure_date": datetime.date(2022, 7, 1),
                    "return_date": datetime.date(2022, 7, 10),
                    "children": 2,
                    "offset": 10,
                },
                {
                    "adults": 1,
                    "children": 2,
                    "infants": 0,
                    "limit": 10,
                    "site": "BR",
                    "channel": "site",
                    "from": "CLT",
                    "to": "TPA",
                    "departureDate": "2022-07-01",
                    "returnDate": "2022-07-10",
                    "viewMode": "CLUSTER",
                    "h": "xxx",
                    "offset": 10,
                    "user": "xxx",
                },
            ),
        ],
    )
    def test_get_params(self, kwargs, expected):
        actual = WebSearchApi(
            h_token="xxx",
            x_uow_token="xxx",
            user_agent="xxx",
            user_id="xxx",
            gui_version="xxx",
            tracking_code="xxx",
        )._get_params(**kwargs)
        assert actual == expected
