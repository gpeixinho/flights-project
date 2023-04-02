import pandas as pd
import pytest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from transformation.jobs import parse_decolar_flights


class TestParseDecolarFlightsJob:
    def test_extract_data(self, spark_session):
        test_config = {
            "source_data_path": "tests/transformation/data/decolar/websearch/input_data.json"
        }
        actual = parse_decolar_flights._extract_data(spark_session, test_config)
        expected_columns = [
            "abTestingString",
            "additionalServices",
            "airlineReviews",
            "airlines",
            "airlinesMatrix",
            "ajax",
            "betterPriceBanner",
            "breakdownType",
            "chargesDespegar",
            "contextDraper",
            "currencies",
            "destination",
            "directFlightsCount",
            "disambiguationType",
            "facetsGroups",
            "hasDvil",
            "hasRefineParameters",
            "highlights",
            "incomeType",
            "initialCurrency",
            "items",
            "nonStopSortingConfig",
            "origin",
            "pagination",
            "promoPaymentsConfig",
            "pushProducts",
            "referenceData",
            "searchDetailText",
            "searchId",
            "showLegalAdvisor",
            "showOneInstallment",
            "showRefineBar",
            "sorting",
            "times",
        ]
        # testing for shape only
        assert actual.columns == expected_columns
        assert actual.count() == 1

    @pytest.mark.skip(reason="test is broken")
    def test_transform_data(self, spark_session):
        df_raw_pandas = pd.read_pickle(
            "tests/transformation/data/decolar/websearch/df_raw.pickle"
        )
        df_raw = spark_session.createDataFrame(df_raw_pandas)
        actual = parse_decolar_flights._transform_data_v2(
            spark_session, df_raw
        ).toPandas()
        expected = pd.read_pickle(
            "tests/transformation/data/decolar/websearch/df_transformed.pickle"
        )
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
