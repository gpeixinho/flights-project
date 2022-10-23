import datetime
import pytest
from ingestion.opensky.apis import AirportFlightsApi
from ingestion.decolar.apis import WebSearchApi
from unittest.mock import patch


class TestAirportFlightsApi:
    @pytest.mark.parametrize(
        "type, expected",
        [
            ("arrival", "https://opensky-network.org/api/flights/arrival"),
            ("departure", "https://opensky-network.org/api/flights/departure"),
        ],
    )
    def test_get_endpoint(self, type, expected):
        api = AirportFlightsApi(username="user", password="pass", airport="SBGR")
        actual = api._get_endpoint(type)
        assert actual == expected

    def test_get_endpoint_type_not_arrival_or_departure(self):
        with pytest.raises(ValueError):
            AirportFlightsApi(
                username="user", password="pass", airport="SBGR"
            )._get_endpoint("not_arrival")

    @pytest.mark.parametrize(
        "datetime, expected",
        [
            (datetime.datetime(2022, 6, 30), 1656558000),
            (datetime.datetime(2022, 7, 1), 1656644400),
            (datetime.datetime(2022, 3, 30), 1648609200),
            (datetime.datetime(2022, 6, 30, 0, 0, 5), 1656558005),
        ],
    )
    def test_get_unix_epoch(self, datetime, expected):
        actual = AirportFlightsApi(
            username="user", password="pass", airport="SBGR"
        )._get_unix_epoch(datetime)
        assert actual == expected

    @pytest.mark.parametrize(
        "airport, begin, end, expected",
        [
            (
                "SBGR",
                datetime.datetime(2022, 6, 30),
                datetime.datetime(2022, 7, 1),
                {"airport": "SBGR", "begin": "1656558000", "end": "1656644400"},
            ),
            (
                "SBSP",
                datetime.datetime(2022, 6, 30),
                datetime.datetime(2022, 7, 1),
                {"airport": "SBSP", "begin": "1656558000", "end": "1656644400"},
            ),
        ],
    )
    def test_get_params(self, airport, expected, begin, end):
        actual = AirportFlightsApi(
            username="user", password="pass", airport=airport
        )._get_params(begin=begin, end=end)
        assert actual == expected


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
