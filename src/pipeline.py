import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingest import (
    discover_files,
    filter_unprocessed,
    build_control_record
)

from schema_manager import (
    prepare_daily_dataframe,
    detect_schema_drift
)

from data_quality import build_quality_metrics
from aggregate import build_daily_aggregate
from analytics import write_analytics


def get_previous_schema(spark, control_path):
    """
    Retrieve the schema columns from the most recently
    successfully processed file.
    """

    try:
        control_df = (
            spark.read
            .parquet(control_path)
            .filter(
                F.col("status") == "SUCCESS"
            )
            .orderBy(
                F.col("processed_at").desc()
            )
            .limit(1)
        )

        row = (
            control_df
            .select("schema_columns")
            .first()
        )

        if row and row["schema_columns"]:
            return row["schema_columns"]

    except Exception:
        return None

    return None


def write_control_record(
    control_df,
    control_path
):
    """
    Append successful processing information
    to the control dataset.
    """

    control_df.write.mode(
        "append"
    ).parquet(control_path)


def main():

    parser = argparse.ArgumentParser(
        description="Backblaze Drive Stats PySpark Pipeline"
    )

    parser.add_argument(
        "--input_path",
        required=True
    )

    parser.add_argument(
        "--aggregate_path",
        required=True
    )

    parser.add_argument(
        "--quality_path",
        required=True
    )

    parser.add_argument(
        "--control_path",
        required=True
    )

    parser.add_argument(
        "--output_path",
        required=True
    )

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BackblazeDriveStats")
        .getOrCreate()
    )

    try:

        # ---------------------------------------------------------
        # 1. Discover available CSV files
        # ---------------------------------------------------------

        files = discover_files(
            spark,
            args.input_path
        )

        print(
            f"Discovered {len(files)} CSV files."
        )

        # ---------------------------------------------------------
        # 2. Identify files that have not been processed
        # ---------------------------------------------------------

        new_files = filter_unprocessed(
            spark,
            files,
            args.control_path
        )

        print(
            f"Files to process: {len(new_files)}"
        )

        if not new_files:
            print(
                "No new files to process."
            )
            return

        # ---------------------------------------------------------
        # 3. Process one daily file at a time
        # ---------------------------------------------------------

        for file_path in new_files:

            print(
                f"Processing file: {file_path}"
            )

            df, schema_metadata = (
                prepare_daily_dataframe(
                    spark,
                    file_path
                )
            )

            # -----------------------------------------------------
            # 4. Detect schema drift
            # -----------------------------------------------------

            previous_schema = get_previous_schema(
                spark,
                args.control_path
            )

            if previous_schema:

                drift = detect_schema_drift(
                    schema_metadata["original_columns"],
                    previous_schema
                )

                schema_metadata.update(
                    drift
                )

                print(
                    "Schema drift detected: "
                    f"{drift['has_schema_drift']}"
                )

                if drift["added_columns"]:
                    print(
                        "Added columns: "
                        f"{drift['added_columns']}"
                    )

                if drift["removed_columns"]:
                    print(
                        "Removed columns: "
                        f"{drift['removed_columns']}"
                    )

            else:

                schema_metadata.update(
                    {
                        "added_columns": [],
                        "removed_columns": [],
                        "has_schema_drift": False
                    }
                )

                print(
                    "No previous schema found. "
                    "Treating this as the initial schema."
                )

            # -----------------------------------------------------
            # 5. Data quality processing
            # -----------------------------------------------------

            quality_df = build_quality_metrics(
                df,
                schema_metadata
            )

            # -----------------------------------------------------
            # 6. Build daily aggregate
            # -----------------------------------------------------

            aggregate_df = build_daily_aggregate(
                df
            )

            # -----------------------------------------------------
            # 7. Write aggregate data
            # -----------------------------------------------------

            (
                aggregate_df
                .write
                .mode("append")
                .partitionBy("drive_date")
                .parquet(args.aggregate_path)
            )

            # -----------------------------------------------------
            # 8. Write quality metrics
            # -----------------------------------------------------

            (
                quality_df
                .write
                .mode("append")
                .parquet(args.quality_path)
            )

            # -----------------------------------------------------
            # 9. Create control record
            # -----------------------------------------------------

            control_df = build_control_record(
                spark,
                file_path,
                df,
                schema_metadata["schema_hash"]
            )

            control_df = (
                control_df
                .withColumn(
                    "schema_columns",
                    F.array(
                        *[
                            F.lit(column)
                            for column
                            in schema_metadata[
                                "original_columns"
                            ]
                        ]
                    )
                )
            )

            # -----------------------------------------------------
            # 10. Mark file as successfully processed
            # -----------------------------------------------------

            write_control_record(
                control_df,
                args.control_path
            )

            print(
                f"Successfully processed: {file_path}"
            )

        # ---------------------------------------------------------
        # 11. Generate analytics
        # ---------------------------------------------------------

        write_analytics(
            spark,
            args.aggregate_path,
            args.quality_path,
            args.output_path
        )

        print(
            "Analytics generated successfully."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()
