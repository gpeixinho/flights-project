from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def _get_null_callsign_count(spark, table):
    return (
        spark.sql(
            f"""
        SELECT COUNT(*) AS cnt 
        FROM {table} 
        WHERE callsign is NULL"""
        )
        .collect()[0]
        .cnt
    )


def _trim_callsign(df: DataFrame):
    return df.withColumn("callsign", trim(col("callsign")))


def _convert_timestamps(df: DataFrame) -> DataFrame:
    return (df.withColumn("firstSeen", from_unixtime(col("firstSeen")))
              .withColumn("lastSeen", from_unixtime(col("lastSeen"))
    ))

def _get_airline_iata(df: DataFrame) -> DataFrame:
    return df.withColumn("airlineIcao", substring(col("callsign"), 1, 3))

def _extract_data(spark: SparkSession, config: dict) -> DataFrame:

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

    df_raw = (
        spark.read.format("json")
        .schema(schema)
        .option("multiLine", "false")
        .load(config.get("source_data_path"))
    )

    return df_raw


def _transform_data(df_raw: DataFrame) -> DataFrame:
    return _get_airline_iata(_convert_timestamps(_trim_callsign(df_raw)))


def _load_data(config: dict, df_transformed: DataFrame) -> None:
    (
        df_transformed.write.format("parquet")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(config.get("output_data_path"))
    )


def run_job(spark: SparkSession, config: dict) -> None:
    _load_data(config, _transform_data(_extract_data(spark, config)))
