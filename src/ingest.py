from pyspark.sql import functions as F


def discover_files(spark, input_path):
    """
    Discover daily CSV files from the input location.

    Only file paths are collected to the driver.
    Raw data is never collected to the driver.
    """

    files_df = (
        spark.read
        .format("binaryFile")
        .option("pathGlobFilter", "*.csv")
        .load(input_path)
        .select("path")
    )

    return [
        row["path"]
        for row in files_df.orderBy("path").collect()
    ]


def filter_unprocessed(spark, files, control_path):
    """
    Return only source files that have not already been
    successfully processed.

    The control data contains:
        source_file
        status
        row_count
        schema_hash
        processed_at
    """

    if not files:
        return []

    candidate_df = spark.createDataFrame(
        [(file_path,) for file_path in files],
        ["source_file"]
    )

    try:
        processed_df = (
            spark.read
            .parquet(control_path)
            .filter(
                F.col("status") == "SUCCESS"
            )
            .select("source_file")
            .distinct()
        )

        new_files_df = (
            candidate_df
            .join(
                processed_df,
                on="source_file",
                how="left_anti"
            )
            .orderBy("source_file")
        )

        return [
            row["source_file"]
            for row in new_files_df.collect()
        ]

    except Exception as exc:

        # On the first run the control location may not exist.
        # In that case all discovered files are candidates.
        print(
            f"Control state not available yet: {exc}"
        )

        return files


def read_daily_csv(spark, file_path):
    """
    Read one daily Backblaze CSV file.

    One file is processed at a time to keep the driver
    memory bounded.
    """

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )


def build_control_record(
    spark,
    file_path,
    df,
    schema_hash,
    status="SUCCESS"
):
    """
    Create a control-state record for a processed file.

    The record should be written only after the output
    for the source file has been successfully created.
    """

    return spark.createDataFrame(
        [
            (
                file_path,
                str(
                    df.select("drive_date")
                    .first()["drive_date"]
                ),
                status,
                df.count(),
                schema_hash
            )
        ],
        [
            "source_file",
            "file_date",
            "status",
            "row_count",
            "schema_hash"
        ]
    ).withColumn(
        "processed_at",
        F.current_timestamp()
    )
