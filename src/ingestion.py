"""
NYC Yellow Taxi Data Pipeline
Ingestion Module

Responsible for reading raw NYC TLC Yellow Taxi
Parquet files into Spark DataFrames.
"""

from pyspark.sql import SparkSession, DataFrame


def create_spark_session() -> SparkSession:
    """
    Create and return the Spark session used by the pipeline.
    """

    spark = (
        SparkSession.builder
        .appName("NYC Taxi Data Pipeline")
        .getOrCreate()
    )

    return spark


def read_taxi_data(
    spark: SparkSession,
    input_path: str
) -> DataFrame:
    """
    Read NYC Yellow Taxi Parquet data.

    The input path can point to:
    - One Parquet file
    - Multiple Parquet files
    - A directory containing monthly Parquet files

    This allows the pipeline to process multiple monthly
    batches without manually combining the files.
    """

    taxi_df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(input_path)
    )

    return taxi_df


def inspect_data(df: DataFrame) -> None:
    """
    Display basic information about the incoming dataset.
    """

    print("\n--- NYC Taxi Dataset ---")

    print("\nSchema:")
    df.printSchema()

    print("\nSample Records:")
    df.show(5, truncate=False)

    print(f"\nTotal Records: {df.count():,}")
