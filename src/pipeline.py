import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ingest import (
    discover_files,
    filter_unprocessed,
    build_control_record
)

from schema_manager import (
    prepare_daily_dataframe
)

from data_quality import (
    build_quality_metrics
)

from aggregate import (
    build_daily_aggregate
)

from analytics import (
    write_analytics
)


def get_previous_schema(spark, control_path):
    """
    Get the most recently processed schema from the control table.

    Returns None when no previous schema exists.
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

        row = control_df.select(
            "schema_columns"
        ).first()

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
    Write successful processing state.

    The control record is written only after the
    corresponding aggregate and quality outputs succeed.
    """

    (
        control_df
        .write
        .mode("append")
        .parquet(control_path)
    )


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

        # -----------------------------------------------------
        # 1. Discover input files
        # -----------------------------------------------------

        files = discover_files(
            spark,
            args.input_path
        )

        print(
            f"Discovered {len(files)} CSV files."
        )

        # -----------------------------------------------------
        # 2. Identify only unprocessed files
        # -----------------------------------------------------

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

        # -----------------------------------------------------
        # 3. Process each daily file
        # -----------------------------------------------------

        for file_path in new_files:

            print(
                f"Processing file: {file_path}"
            )

            # Read and normalize one file only.
            df, schema_metadata = (
                prepare_daily_dataframe(
                    spark,
                    file_path
                )
            )

            # -------------------------------------------------
            # Schema drift detection
            # -------------------------------------------------

            previous_schema = (
                get_previous_schema(
                    spark,
                    args.control_path
                )
            )

            if previous_schema:

                current_schema = (
                    schema_metadata["columns"]
                )

                schema_metadata[
                    "has_schema_drift"
                ] = (
                    set(current_schema)
                    != set(previous_schema)
                )

            else:

                schema_metadata[
                    "has_schema_drift"
                ] = False

            # -------------------------------------------------
            # Data quality
            # -------------------------------------------------

            quality_df = build_quality_metrics(
                df,
                schema_metadata
            )

            # -------------------------------------------------
            # Daily aggregation
            # -------------------------------------------------

            aggregate_df = build_daily_aggregate(
                df
            )

            # -------------------------------------------------
            # Write aggregate output
            # -------------------------------------------------

            (
                aggregate_df
                .write
                .mode("append")
                .partitionBy("drive_date")
                .parquet(
                    args.aggregate_path
                )
            )

            # -------------------------------------------------
            # Write quality output
            # -------------------------------------------------

            (
                quality_df
                .write
                .mode("append")
                .parquet(
                    args.quality_path
                )
            )

            # -------------------------------------------------
            # Create control record
            # -------------------------------------------------

            control_df = (
                build_control_record(
                    spark,
                    file_path,
                    df,
                    schema_metadata[
                        "schema_hash"
                    ]
                )
                .withColumn(
                    "schema_columns",
                    F.array(
                        *[
                            F.lit(column)
                            for column
                            in schema_metadata[
                                "columns"
                            ]
                        ]
                    )
                )
            )

            # -------------------------------------------------
            # Mark source file SUCCESS
            # -------------------------------------------------

            write_control_record(
                control_df,
                args.control_path
            )

            print(
                f"Successfully processed: {file_path}"
            )

        # -----------------------------------------------------
        # 4. Generate analytics from aggregate layer
        # -----------------------------------------------------

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
