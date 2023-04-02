import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def _extract_data(spark: SparkSession, config: dict) -> DataFrame:

    schema = (
        StructType()
        .add("iata", StringType(), True)
        .add("icao", StringType(), True)
        .add("airline", StringType(), True)
        .add("callsign", StringType(), True)
        .add("country_region", StringType(), True)
        .add("comments", StringType(), True)
    )

    df = (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "false")
        .option("enforceSchema", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("mode", "DROPMALFORMED")
        .load(f"{config.get('source_data_path')}\\*.csv")
        .filter("iata != 'IATA'") # remove old header
    )

    return df

def _transform_data(df_raw: DataFrame) -> DataFrame:
    return df_raw # no transformations implemented so far


def _load_data(config: dict, df: DataFrame) -> None:

    (
        df.coalesce(1)
        .write.format("parquet")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(
            f"{config.get('output_data_path')}\\processed_at={datetime.datetime.now().date()}"
        )
    )


def run_job(spark: SparkSession, config: dict) -> None:
    _load_data(config, _transform_data(_extract_data(spark, config)))
