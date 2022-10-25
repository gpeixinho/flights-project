import datetime
import pytest
from ingestion.opensky.apis import AirportFlightsApi

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