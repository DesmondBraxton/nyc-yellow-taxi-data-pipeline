"""
NYC Yellow Taxi Data Pipeline
Data Quality Module

Responsible for validating incoming taxi records
and separating valid records from invalid records.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_taxi_data(df: DataFrame):
    """
    Split the incoming dataset into valid and invalid records.

    Records are sent to quarantine if they have:
    - missing pickup or dropoff timestamps
    - missing trip distance
    - missing fare or total amount
    - zero or negative trip distance
    - negative fare amount
    - negative total amount
    """

    invalid_condition = (
        F.col("tpep_pickup_datetime").isNull()
        | F.col("tpep_dropoff_datetime").isNull()
        | F.col("trip_distance").isNull()
        | F.col("fare_amount").isNull()
        | F.col("total_amount").isNull()
        | (F.col("trip_distance") <= 0)
        | (F.col("fare_amount") < 0)
        | (F.col("total_amount") < 0)
    )

    invalid_df = df.filter(invalid_condition)

    valid_df = df.filter(~invalid_condition)

    return valid_df, invalid_df


def add_quality_flags(df: DataFrame) -> DataFrame:
    """
    Add quality flags for suspicious records that
    may be useful for later analysis.
    """

    flagged_df = (
        df.withColumn(
            "invalid_passenger_count",
            F.when(
                (F.col("passenger_count") <= 0)
                | F.col("passenger_count").isNull(),
                True
            ).otherwise(False)
        )
        .withColumn(
            "unusually_long_trip",
            F.when(
                F.col("trip_distance") > 100,
                True
            ).otherwise(False)
        )
    )

    return flagged_df
