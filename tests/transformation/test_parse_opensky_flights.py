import pandas as pd
import pytest
from pyspark.sql.types import *
from transformation.jobs import parse_opensky_flights


@pytest.fixture
def df_raw(spark_session):
    schema = (
        StructType()
        .add("icao24", StringType(), True)
        .add("firstSeen", LongType(), True)
        .add("estDepartureAirport", StringType(), True)
        .add("lastSeen", LongType(), True)
        .add("estArrivalAirport", StringType(), True)
        .add("callsign", StringType(), True)
        .add("estDepartureAirportHorizDistance", LongType(), True)
        .add("estDepartureAirportVertDistance", LongType(), True)
        .add("estArrivalAirportHorizDistance", LongType(), True)
        .add("estArrivalAirportVertDistance", LongType(), True)
        .add("departureAirportCandidatesCount", IntegerType(), True)
        .add("arrivalAirportCandidatesCount", IntegerType(), True)
    )

    data = [
        (
            "78042c",
            1655783172,
            "RCTP",
            1655786511,
            None,
            "CXA888  ",
            14108,
            1552,
            None,
            None,
            0,
            0,
        ),
        (
            "040142",
            1655783027,
            "OMSJ",
            1655784208,
            None,
            "ETH3613 ",
            910,
            141,
            None,
            None,
            0,
            0,
        ),
        (
            "7812f3",
            1655782418,
            None,
            1655783991,
            None,
            "CXA8319 ",
            None,
            None,
            None,
            None,
            0,
            0,
        ),
    ]

    return spark_session.createDataFrame(data, schema=schema)


class TestParseOpenSkyFlights:
    def test_extract_data(self, spark_session, df_raw):
        test_config = {
            "source_data_path": "tests/transformation/data/opensky/input_data.json"
        }
        actual = parse_opensky_flights._extract_data(
            spark_session, test_config
        ).toPandas()

        expected = df_raw.toPandas()

        pd.testing.assert_frame_equal(actual, expected)

    def test_trim_callsign(self, spark_session, df_raw):
        actual = parse_opensky_flights._trim_callsign(df_raw).toPandas()
        expected = spark_session.createDataFrame(
            schema=StructType()
            .add("icao24", StringType(), True)
            .add("firstSeen", LongType(), True)
            .add("estDepartureAirport", StringType(), True)
            .add("lastSeen", LongType(), True)
            .add("estArrivalAirport", StringType(), True)
            .add("callsign", StringType(), True)
            .add("estDepartureAirportHorizDistance", LongType(), True)
            .add("estDepartureAirportVertDistance", LongType(), True)
            .add("estArrivalAirportHorizDistance", LongType(), True)
            .add("estArrivalAirportVertDistance", LongType(), True)
            .add("departureAirportCandidatesCount", IntegerType(), True)
            .add("arrivalAirportCandidatesCount", IntegerType(), True),
            data=[
                (
                    "78042c",
                    1655783172,
                    "RCTP",
                    1655786511,
                    None,
                    "CXA888",
                    14108,
                    1552,
                    None,
                    None,
                    0,
                    0,
                ),
                (
                    "040142",
                    1655783027,
                    "OMSJ",
                    1655784208,
                    None,
                    "ETH3613",
                    910,
                    141,
                    None,
                    None,
                    0,
                    0,
                ),
                (
                    "7812f3",
                    1655782418,
                    None,
                    1655783991,
                    None,
                    "CXA8319",
                    None,
                    None,
                    None,
                    None,
                    0,
                    0,
                ),
            ],
        ).toPandas()

        pd.testing.assert_frame_equal(actual, expected)

    def test_convert_timestamps(self, spark_session, df_raw):
        expected = spark_session.createDataFrame(
            schema=(
                StructType()
                .add("icao24", StringType(), True)
                .add("firstSeen", StringType(), True)
                .add("estDepartureAirport", StringType(), True)
                .add("lastSeen", StringType(), True)
                .add("estArrivalAirport", StringType(), True)
                .add("callsign", StringType(), True)
                .add("estDepartureAirportHorizDistance", LongType(), True)
                .add("estDepartureAirportVertDistance", LongType(), True)
                .add("estArrivalAirportHorizDistance", LongType(), True)
                .add("estArrivalAirportVertDistance", LongType(), True)
                .add("departureAirportCandidatesCount", IntegerType(), True)
                .add("arrivalAirportCandidatesCount", IntegerType(), True)
            ),
            data=[
                (
                    "78042c",
                    "2022-06-21 00:46:12",
                    "RCTP",
                    "2022-06-21 01:41:51",
                    None,
                    "CXA888  ",
                    14108,
                    1552,
                    None,
                    None,
                    0,
                    0,
                ),
                (
                    "040142",
                    "2022-06-21 00:43:47",
                    "OMSJ",
                    "2022-06-21 01:03:28",
                    None,
                    "ETH3613 ",
                    910,
                    141,
                    None,
                    None,
                    0,
                    0,
                ),
                (
                    "7812f3",
                    "2022-06-21 00:33:38",
                    None,
                    "2022-06-21 00:59:51",
                    None,
                    "CXA8319 ",
                    None,
                    None,
                    None,
                    None,
                    0,
                    0,
                ),
            ],
        ).toPandas()
        actual = parse_opensky_flights._convert_timestamps(df_raw).toPandas()
        pd.testing.assert_frame_equal(actual, expected)

    def test_get_airline_iata(self, spark_session, df_raw):
        expected = spark_session.createDataFrame(
             schema=(
                StructType()
                .add("icao24", StringType(), True)
                .add("firstSeen", LongType(), True)
                .add("estDepartureAirport", StringType(), True)
                .add("lastSeen", LongType(), True)
                .add("estArrivalAirport", StringType(), True)
                .add("callsign", StringType(), True)
                .add("estDepartureAirportHorizDistance", LongType(), True)
                .add("estDepartureAirportVertDistance", LongType(), True)
                .add("estArrivalAirportHorizDistance", LongType(), True)
                .add("estArrivalAirportVertDistance", LongType(), True)
                .add("departureAirportCandidatesCount", IntegerType(), True)
                .add("arrivalAirportCandidatesCount", IntegerType(), True)
                .add("airlineIcao", StringType(), True)
            ),
            data=[
                (
                    "78042c",
                    1655783172,
                    "RCTP",
                    1655786511,
                    None,
                    "CXA888  ",
                    14108,
                    1552,
                    None,
                    None,
                    0,
                    0,
                    "CXA"
                ),
                (
                    "040142",
                    1655783027,
                    "OMSJ",
                    1655784208,
                    None,
                    "ETH3613 ",
                    910,
                    141,
                    None,
                    None,
                    0,
                    0,
                    "ETH"
                ),
                (
                    "7812f3",
                    1655782418,
                    None,
                    1655783991,
                    None,
                    "CXA8319 ",
                    None,
                    None,
                    None,
                    None,
                    0,
                    0,
                    "CXA"
                ),
            ]

        ).toPandas()
        actual = parse_opensky_flights._get_airline_iata(df_raw).toPandas()
        pd.testing.assert_frame_equal(actual, expected)

    def test_transform_data(self, spark_session, df_raw):
        expected = spark_session.createDataFrame(
             schema=(
                StructType()
                .add("icao24", StringType(), True)
                .add("firstSeen", StringType(), True)
                .add("estDepartureAirport", StringType(), True)
                .add("lastSeen", StringType(), True)
                .add("estArrivalAirport", StringType(), True)
                .add("callsign", StringType(), True)
                .add("estDepartureAirportHorizDistance", LongType(), True)
                .add("estDepartureAirportVertDistance", LongType(), True)
                .add("estArrivalAirportHorizDistance", LongType(), True)
                .add("estArrivalAirportVertDistance", LongType(), True)
                .add("departureAirportCandidatesCount", IntegerType(), True)
                .add("arrivalAirportCandidatesCount", IntegerType(), True)
                .add("airlineIcao", StringType(), True)
            ),
            data=[
                (
                    "78042c",
                    "2022-06-21 00:46:12",
                    "RCTP",
                    "2022-06-21 01:41:51",
                    None,
                    "CXA888",
                    14108,
                    1552,
                    None,
                    None,
                    0,
                    0,
                    "CXA"
                ),
                (
                    "040142",
                    "2022-06-21 00:43:47",
                    "OMSJ",
                    "2022-06-21 01:03:28",
                    None,
                    "ETH3613",
                    910,
                    141,
                    None,
                    None,
                    0,
                    0,
                    "ETH"
                ),
                (
                    "7812f3",
                    "2022-06-21 00:33:38",
                    None,
                    "2022-06-21 00:59:51",
                    None,
                    "CXA8319",
                    None,
                    None,
                    None,
                    None,
                    0,
                    0,
                    "CXA"
                ),
            ]).toPandas()
        actual = parse_opensky_flights._transform_data(df_raw).toPandas()
        pd.testing.assert_frame_equal(actual, expected)

        

        
