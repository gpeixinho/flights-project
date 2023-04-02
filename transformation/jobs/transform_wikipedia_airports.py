import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def _split_daylight_saving_period(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "daylightSavingPeriod", translate(col("daylightSavingPeriod"), "–", "-")
        )  # some rows are separated by this character instead of '-'
        .withColumn(
            "monthDaylightSavingStart", split(col("daylightSavingPeriod"), "-")[0]
        )
        .withColumn(
            "monthDaylightSavingEnd", split(col("daylightSavingPeriod"), "-")[1]
        )
        .withColumn(
            "monthDaylightSavingEnd", regexp_replace("monthDaylightSavingEnd", "\d", "")
        )  # some rows contain a digit at the end because of wikipedia references
        .withColumn(
            "monthDaylightSavingStart",
            from_unixtime(unix_timestamp(col("monthDaylightSavingStart"), "MMM"), "MM"),
        )  # get month number
        .withColumn(
            "monthDaylightSavingEnd",
            from_unixtime(unix_timestamp(col("monthDaylightSavingEnd"), "MMM"), "MM"),
        )
    )


def _clean_airport_name(df: DataFrame) -> DataFrame:
    return df.withColumn("airportName", translate(col("airportName"), "[1]", ""))


def _transform_data(df_raw: DataFrame) -> DataFrame:
    df_transformed = _split_daylight_saving_period(_clean_airport_name(df_raw))
    return df_transformed


def _extract_data(spark: SparkSession, config: dict) -> DataFrame:

    schema = (
        StructType()
        .add("iata", StringType(), True)
        .add("icao", StringType(), True)
        .add("airportName", StringType(), True)
        .add("locationServed", StringType(), True)
        .add("timezone", StringType(), True)
        .add("daylightSavingPeriod", StringType(), True)
    )

    df = (
        spark.read.format("csv")
        .schema(schema)
        .option("comment", "-")
        .option("header", "false")
        .option("mode", "DROPMALFORMED")
        .load(f"{config.get('source_data_path')}/extracted_at={config.get('extract_date')}/*")
        .filter("iata != 'IATA'") # remove old header
    )

    return df


def _load_data(config: dict, df: DataFrame) -> None:

    (
        df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(
            f"{config.get('output_data_path')}/processed_at={datetime.datetime.now().date()}"
        )
    )


def run_job(spark: SparkSession, config: dict) -> None:
    _load_data(config, _transform_data(_extract_data(spark, config)))
