import datetime
import pytest

from unittest.mock import patch
from ingestion.opensky.apis import OpenSkyApi
from ingestion.opensky.apis import AirportFlightsApi
from ingestion.opensky.apis import AllFlightsApi


class TestOpenSkyApi:
    @patch("ingestion.opensky.apis.OpenSkyApi.__abstractmethods__", set())
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
        actual = OpenSkyApi(username="user", password="pass")._get_unix_epoch(datetime)
        assert actual == expected


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


class TestAllFlightsApi:
    def test_get_endpoint(self):
        api = AllFlightsApi(username="user", password="pass")
        expected = "https://opensky-network.org/api/flights/all"
        actual = api._get_endpoint()
        assert actual == expected

    @pytest.mark.parametrize(
        "begin, end, expected",
        [
            (
                datetime.datetime(2022, 6, 30, 1),
                datetime.datetime(2022, 6, 30, 3),
                {"begin": "1656561600", "end": "1656568800"},
            )
        ],
    )
    def test_get_params(self, begin, end, expected):
        api = AllFlightsApi(username="user", password="pass")
        actual = api._get_params(
            begin=begin, end=end
        )
        assert actual == expected

    def test_get_params_begin_later_than_now(self):
        end = datetime.datetime.now()
        begin = datetime.datetime.now() + datetime.timedelta(minutes=2)
        with pytest.raises(RuntimeError) as exception_info:
            api = AllFlightsApi(username="user", password="pass")
            api._get_params(begin=begin, end=end)
        assert exception_info.match("begin cannot be greater than end")

    def test_get_params_time_interval_greater_than_2_hours(self):
        begin = datetime.datetime.now()
        end = datetime.datetime.now() + datetime.timedelta(hours=2, seconds=1)
        with pytest.raises(RuntimeError) as exception_info:
            api = AllFlightsApi(username="user", password="pass")
            api._get_params(begin=begin, end=end)
        assert exception_info.match("the time interval cannot be greater than 2 hours") 