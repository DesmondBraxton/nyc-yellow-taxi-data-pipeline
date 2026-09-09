"""
NYC Yellow Taxi Data Pipeline
Transformation Module

Responsible for cleaning valid taxi records
and creating derived analytical fields.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_taxi_data(df: DataFrame) -> DataFrame:
    """
    Apply basic cleaning rules to valid taxi records.
    """

    cleaned_df = (
        df.dropDuplicates()
        .filter(F.col("trip_distance") > 0)
        .filter(F.col("fare_amount") >= 0)
        .filter(F.col("total_amount") >= 0)
    )

    return cleaned_df


def add_trip_features(df: DataFrame) -> DataFrame:
    """
    Create new fields used for downstream analytics.
    """

    transformed_df = (
        df.withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            ) / 60
        )
        .withColumn(
            "pickup_date",
            F.to_date("tpep_pickup_datetime")
        )
        .withColumn(
            "dropoff_date",
            F.to_date("tpep_dropoff_datetime")
        )
        .withColumn(
            "pickup_year",
            F.year("tpep_pickup_datetime")
        )
        .withColumn(
            "pickup_month",
            F.month("tpep_pickup_datetime")
        )
        .withColumn(
            "pickup_day",
            F.dayofmonth("tpep_pickup_datetime")
        )
        .withColumn(
            "pickup_hour",
            F.hour("tpep_pickup_datetime")
        )
        .withColumn(
            "tip_percentage",
            F.when(
                F.col("fare_amount") > 0,
                (F.col("tip_amount") / F.col("fare_amount")) * 100
            ).otherwise(0)
        )
    )

    return transformed_df


def remove_invalid_durations(df: DataFrame) -> DataFrame:
    """
    Remove trips where dropoff occurs before pickup
    or where calculated duration is zero.
    """

    return df.filter(
        F.col("trip_duration_minutes") > 0
    )


def create_daily_summary(df: DataFrame) -> DataFrame:
    """
    Create daily-level taxi metrics.
    """

    daily_summary_df = (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_duration_minutes").alias(
                "avg_trip_duration_minutes"
            )
        )
        .orderBy("pickup_date")
    )

    return daily_summary_df
