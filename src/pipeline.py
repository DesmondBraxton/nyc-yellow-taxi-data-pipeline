"""
NYC Yellow Taxi Data Pipeline
Main Pipeline Orchestration

Coordinates ingestion, data quality, transformation,
enrichment, and output steps.
"""

import os

from ingestion import create_spark_session, read_taxi_data, inspect_data
from data_quality import validate_taxi_data, add_quality_flags
from transformations import (
    clean_taxi_data,
    add_trip_features,
    remove_invalid_durations,
    create_daily_summary,
)
from enrichment import read_zone_lookup, add_taxi_zones


def main():
    """
    Run the NYC Yellow Taxi ETL pipeline.
    """

    raw_taxi_path = os.path.join(
        "data",
        "raw",
        "yellow_taxi"
    )

    zone_lookup_path = os.path.join(
        "data",
        "reference",
        "taxi_zone_lookup.csv"
    )

    curated_output_path = os.path.join(
        "data",
        "curated",
        "yellow_taxi"
    )

    quarantine_output_path = os.path.join(
        "data",
        "quarantine",
        "yellow_taxi"
    )

    daily_summary_output_path = os.path.join(
        "data",
        "analytics",
        "daily_summary"
    )

    spark = create_spark_session()

    try:
        # -------------------------------------------------
        # 1. INGESTION
        # -------------------------------------------------
        taxi_df = read_taxi_data(
            spark,
            raw_taxi_path
        )

        inspect_data(taxi_df)

        # -------------------------------------------------
        # 2. DATA QUALITY
        # -------------------------------------------------
        valid_df, invalid_df = validate_taxi_data(
            taxi_df
        )

        invalid_df = add_quality_flags(
            invalid_df
        )

        # -------------------------------------------------
        # 3. CLEANING
        # -------------------------------------------------
        clean_df = clean_taxi_data(
            valid_df
        )

        # -------------------------------------------------
        # 4. FEATURE ENGINEERING
        # -------------------------------------------------
        transformed_df = add_trip_features(
            clean_df
        )

        transformed_df = remove_invalid_durations(
            transformed_df
        )

        # -------------------------------------------------
        # 5. TAXI ZONE ENRICHMENT
        # -------------------------------------------------
        zone_df = read_zone_lookup(
            spark,
            zone_lookup_path
        )

        enriched_df = add_taxi_zones(
            transformed_df,
            zone_df
        )

        # -------------------------------------------------
        # 6. ANALYTICAL SUMMARY
        # -------------------------------------------------
        daily_summary_df = create_daily_summary(
            enriched_df
        )

        # -------------------------------------------------
        # 7. CURATED OUTPUT
        # -------------------------------------------------
        (
            enriched_df.write
            .mode("overwrite")
            .partitionBy(
                "pickup_year",
                "pickup_month"
            )
            .parquet(
                curated_output_path
            )
        )

        # -------------------------------------------------
        # 8. QUARANTINE OUTPUT
        # -------------------------------------------------
        (
            invalid_df.write
            .mode("overwrite")
            .parquet(
                quarantine_output_path
            )
        )

        # -------------------------------------------------
        # 9. DAILY ANALYTICS OUTPUT
        # -------------------------------------------------
        (
            daily_summary_df.write
            .mode("overwrite")
            .parquet(
                daily_summary_output_path
            )
        )

        print(
            "\n--- Pipeline Completed Successfully ---"
        )

        print(
            f"Valid records: {enriched_df.count():,}"
        )

        print(
            f"Quarantined records: {invalid_df.count():,}"
        )

        print(
            f"Curated output: {curated_output_path}"
        )

    except Exception as error:
        print(
            "\n--- Pipeline Failed ---"
        )

        print(
            f"Error: {error}"
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
