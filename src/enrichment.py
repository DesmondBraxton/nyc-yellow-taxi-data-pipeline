"""
NYC Yellow Taxi Data Pipeline
Enrichment Module

Enriches taxi trip records with pickup and dropoff
borough and taxi-zone information.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def read_zone_lookup(
    spark: SparkSession,
    zone_lookup_path: str
) -> DataFrame:
    """
    Read the NYC TLC Taxi Zone Lookup CSV.
    """

    zone_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zone_lookup_path)
    )

    return zone_df


def add_taxi_zones(
    taxi_df: DataFrame,
    zone_df: DataFrame
) -> DataFrame:
    """
    Add pickup and dropoff taxi-zone information.
    """

    pickup_zones = zone_df.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
        F.col("service_zone").alias("pickup_service_zone")
    )

    dropoff_zones = zone_df.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("service_zone").alias("dropoff_service_zone")
    )

    enriched_df = (
        taxi_df
        .join(
            F.broadcast(pickup_zones),
            on="PULocationID",
            how="left"
        )
        .join(
            F.broadcast(dropoff_zones),
            on="DOLocationID",
            how="left"
        )
    )

    return enriched_df
