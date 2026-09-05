import argparse

from pyspark.sql import SparkSession

from ingest import discover_files, filter_unprocessed
from schema_manager import prepare_daily_dataframe
from data_quality import build_quality_metrics
from aggregate import build_daily_aggregate
from analytics import write_analytics


def main():
    parser = argparse.ArgumentParser(
        description="Backblaze Drive Stats PySpark Pipeline"
    )

    parser.add_argument("--input_path", required=True)
    parser.add_argument("--aggregate_path", required=True)
    parser.add_argument("--quality_path", required=True)
    parser.add_argument("--control_path", required=True)
    parser.add_argument("--output_path", required=True)

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("BackblazeDriveStats")
        .getOrCreate()
    )

    try:
        files = discover_files(spark, args.input_path)

        new_files = filter_unprocessed(
            spark,
            files,
            args.control_path
        )

        if not new_files:
            print("No new files to process.")
            return

        for file_path in new_files:

            print(f"Processing file: {file_path}")

            df, schema_metadata = prepare_daily_dataframe(
                spark,
                file_path
            )

            quality_df = build_quality_metrics(
                df,
                schema_metadata
            )

            aggregate_df = build_daily_aggregate(df)

            aggregate_df.write \
                .mode("append") \
                .partitionBy("drive_date") \
                .parquet(args.aggregate_path)

            quality_df.write \
                .mode("append") \
                .parquet(args.quality_path)

            print(f"Successfully processed: {file_path}")

        write_analytics(
            spark,
            args.aggregate_path,
            args.quality_path,
            args.output_path
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
