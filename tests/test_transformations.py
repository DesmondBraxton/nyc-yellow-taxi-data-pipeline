"""
Tests for the NYC Yellow Taxi Data Pipeline.

These tests validate some of the main transformation
and data-quality rules used in the PySpark pipeline.
"""

import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

SRC_PATH = os.path.join(
    PROJECT_ROOT,
    "src"
)

sys.path.insert(0, SRC_PATH)

from data_quality import validate_taxi_data
from transformations import (
    add_trip_features,
    remove_invalid_durations,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("NYC Taxi Pipeline Tests")
        .getOrCreate()
    )

    yield spark_session

    spark_session.stop()


def test_validate_taxi_data(spark):
    data = [
        (
            datetime(2025, 1, 1, 10, 0),
            datetime(2025, 1, 1, 10, 20),
            5.0,
            20.0,
            25.0,
        ),
        (
            datetime(2025, 1, 1, 11, 0),
            datetime(2025, 1, 1, 11, 10),
            -2.0,
            10.0,
            15.0,
        ),
    ]

    columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    valid_df, invalid_df = validate_taxi_data(
        df
    )

    assert valid_df.count() == 1
    assert invalid_df.count() == 1


def test_validate_null_trip_distance(spark):
    data = [
        (
            datetime(2025, 1, 1, 10, 0),
            datetime(2025, 1, 1, 10, 20),
            None,
            20.0,
            25.0,
        )
    ]

    columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    valid_df, invalid_df = validate_taxi_data(
        df
    )

    assert valid_df.count() == 0
    assert invalid_df.count() == 1


def test_add_trip_features(spark):
    data = [
        (
            datetime(2025, 1, 15, 10, 0),
            datetime(2025, 1, 15, 10, 30),
            20.0,
            4.0,
        )
    ]

    columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "tip_amount",
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    transformed_df = add_trip_features(
        df
    )

    row = transformed_df.first()

    assert row.trip_duration_minutes == 30.0
    assert row.pickup_year == 2025
    assert row.pickup_month == 1
    assert row.pickup_day == 15
    assert row.pickup_hour == 10
    assert row.tip_percentage == 20.0


def test_remove_invalid_durations(spark):
    data = [
        (15.0,),
        (0.0,),
        (-5.0,),
    ]

    columns = [
        "trip_duration_minutes"
    ]

    df = spark.createDataFrame(
        data,
        columns
    )

    result_df = remove_invalid_durations(
        df
    )

    results = result_df.collect()

    assert len(results) == 1
    assert results[0].trip_duration_minutes == 15.0
