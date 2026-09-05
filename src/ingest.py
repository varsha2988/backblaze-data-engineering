from pyspark.sql import functions as F


def discover_files(spark, input_path):
    """
    Discover CSV files from the input location.

    In AWS Glue, input_path can be an S3 prefix.
    """

    files = (
        spark.read
        .format("binaryFile")
        .option("pathGlobFilter", "*.csv")
        .load(input_path)
        .select("path")
        .orderBy("path")
        .collect()
    )

    return [row["path"] for row in files]


def filter_unprocessed(spark, files, control_path):
    """
    Process only files that are not already marked SUCCESS.

    The control table contains:
    source_file, status, row_count, schema_hash, processed_at
    """

    if not files:
        return []

    try:
        processed = (
            spark.read
            .parquet(control_path)
            .filter(F.col("status") == "SUCCESS")
            .select("source_file")
            .distinct()
        )

        candidate_df = spark.createDataFrame(
            [(file_path,) for file_path in files],
            ["source_file"]
        )

        new_files = (
            candidate_df
            .join(processed, on="source_file", how="left_anti")
            .select("source_file")
            .orderBy("source_file")
            .collect()
        )

        return [row["source_file"] for row in new_files]

    except Exception:
        # First run: control table does not exist.
        return files


def read_daily_csv(spark, file_path):
    """
    Read one daily Backblaze CSV.
    """

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )
