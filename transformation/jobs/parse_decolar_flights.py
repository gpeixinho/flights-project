from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession


def _extract_data(spark: SparkSession, config: dict) -> DataFrame:

    df_raw = (
        spark.read.format("json")
        .option("multiLine", "false")
        .load(config.get("source_data_path"))
    )

    return df_raw

def _extract_items_from_raw(spark: SparkSession, df_raw: DataFrame) -> DataFrame:
    df_raw.createOrReplaceTempView("df_raw")
    df_items = spark.sql(
        """
        SELECT EXPLODE(items) AS item
        FROM df_raw
        """
    )
    return df_items

def _extract_route_choices_from_items(spark: SparkSession, df_items: DataFrame) -> DataFrame:
    df_items.createOrReplaceTempView("df_items")
    df_route_choices = spark.sql(
        """
        SELECT item.item.id AS id, EXPLODE(item.item.routeChoices) AS routeChoice
        FROM df_items
        """
    )
    return df_route_choices

def _extract_routes_from_route_choices(spark: SparkSession, df_route_choices: DataFrame) -> DataFrame:
    df_route_choices.createOrReplaceTempView("df_route_choices")
    df_routes = spark.sql(
        """
        SELECT id, EXPLODE(routeChoice.routes) AS route
        FROM df_route_choices
        """
    )

    return df_routes

def _extract_segments_from_routes(spark: SparkSession, df_routes: DataFrame) -> DataFrame:
    df_routes.createOrReplaceTempView("df_routes")
    df_segments = spark.sql(
        """
        SELECT id, EXPLODE(route.segments) AS segment
        FROM df_routes
        """
    )

    return df_segments

def _extract_flights_from_segments(spark: SparkSession, df_segments: DataFrame) -> DataFrame:
    df_segments.createOrReplaceTempView("df_segments")
    df_flights = spark.sql(
        """
        SELECT 
            regexp_extract(id, '!.*', 0) AS id_number,
            segment.departure.airportCode AS departureAirportCode,
            TO_TIMESTAMP(segment.departure.date, "yyyy-MM-dd'T'HH:mm:ssXXX") AS departureDate,
            segment.arrival.airportCode AS arrivalAirportCode,
            TO_TIMESTAMP(segment.arrival.date, "yyyy-MM-dd'T'HH:mm:ssXXX") AS arrivalDate,
            segment.duration AS duration,
            segment.airlineCode AS airlineCode,
            segment.flightId AS flightId,
            segment.operatedBy AS operatedBy,
            segment.cabinType AS cabinType,
            segment.equipmentCode AS equipmentCode,
            segment.nightFlight as nightFlight,
            segment.daysDifference as dayDifference
        FROM df_segments
        """
    )

    return df_flights

def _transform_data_v2(spark: SparkSession, df_raw: DataFrame) -> DataFrame:
    df_raw.createOrReplaceTempView("df_raw")
    df_transformed =  _extract_flights_from_segments(spark, \
                      _extract_segments_from_routes(spark, \
                      _extract_routes_from_route_choices(spark, \
                      _extract_route_choices_from_items(spark, \
                      _extract_items_from_raw(spark, \
                      df_raw)))))
    return df_transformed

def _transform_data(spark: SparkSession, df_raw: DataFrame) -> DataFrame:

    df_raw.createOrReplaceTempView("df_raw")
    df_transformed = spark.sql(
        """
        WITH items AS (
            SELECT EXPLODE(items) AS item
            FROM df_raw
        ),
        routeChoices AS (
            SELECT item.item.id AS id, EXPLODE(item.item.routeChoices) AS routeChoice
            FROM items
        ),
        routes AS (
            SELECT id, EXPLODE(routeChoice.routes) AS route
            FROM routeChoices
        ),
        segments AS (
            SELECT id, EXPLODE(route.segments) AS segment
            FROM routes
        )
        SELECT 
            regexp_extract(id, '!.*', 0) AS id_number,
            segment.departure.airportCode AS departureAirportCode,
            TO_TIMESTAMP(segment.departure.date, "yyyy-MM-dd'T'HH:mm:ssxxx") AS departureDate,
            segment.arrival.airportCode AS arrivalAirportCode,
            TO_TIMESTAMP(segment.arrival.date, "yyyy-MM-dd'T'HH:mm:ssxxx") AS arrivalDate,
            segment.duration AS duration,
            segment.airlineCode AS airlineCode,
            segment.flightId AS flightId,
            segment.operatedBy AS operatedBy,
            segment.cabinType AS cabinType,
            segment.equipmentCode AS equipmentCode,
            segment.nightFlight as nightFlight,
            segment.daysDifference as dayDifference
        FROM segments 
        """
    )

    return df_transformed


def _load_data(config: dict, df_transformed: DataFrame) -> None:
    (
        df_transformed.coalesce(1).write.format("parquet")
        .mode("overwrite")
        .option("compression", "snappy")
        .save(config.get("output_data_path"))
    )


def run_job(spark: SparkSession, config: dict) -> None:
    _load_data(config, _transform_data_v2(spark, _extract_data(spark, config)))
