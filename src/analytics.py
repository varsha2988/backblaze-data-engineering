from pyspark.sql import functions as F


def write_analytics(
    spark,
    aggregate_path,
    quality_path,
    output_path
):
    """
    Generate the required assessment analytics from the
    compact aggregate and quality layers.

    The raw CSV history is not rescanned here.
    """

    aggregate_df = (
        spark.read
        .parquet(aggregate_path)
    )

    # ---------------------------------------------------------
    # 1. AFR by model
    # ---------------------------------------------------------
    #
    # AFR = failures / drive-days * 365.25 * 100

    afr_by_model = (
        aggregate_df
        .groupBy("model")
        .agg(
            F.sum("drive_days").alias("drive_days"),
            F.sum("failure_count").alias("failure_count")
        )
        .withColumn(
            "afr_percent",
            F.when(
                F.col("drive_days") > 0,
                F.col("failure_count")
                / F.col("drive_days")
                * 365.25
                * 100
            ).otherwise(0)
        )
    )

    # ---------------------------------------------------------
    # 2. Top 10 reliable models
    # ---------------------------------------------------------

    top_10_reliable = (
        afr_by_model
        .orderBy(
            F.col("afr_percent").asc()
        )
        .limit(10)
    )

    # ---------------------------------------------------------
    # 3. Top 10 least reliable models
    # ---------------------------------------------------------

    top_10_unreliable = (
        afr_by_model
        .orderBy(
            F.col("afr_percent").desc()
        )
        .limit(10)
    )

    # ---------------------------------------------------------
    # 4. Manufacturer statistics
    # ---------------------------------------------------------

    manufacturer_stats = (
        aggregate_df
        .groupBy("manufacturer")
        .agg(
            F.sum("drive_days").alias("drive_days"),
            F.sum("failure_count").alias("failure_count")
        )
        .orderBy(
            F.col("failure_count").desc()
        )
    )

    # ---------------------------------------------------------
    # 5. Monthly failure trend
    # ---------------------------------------------------------

    monthly_failure_trend = (
        aggregate_df
        .withColumn(
            "month",
            F.date_format(
                F.col("drive_date"),
                "yyyy-MM"
            )
        )
        .groupBy("month")
        .agg(
            F.sum("drive_days").alias("drive_days"),
            F.sum("failure_count").alias("failure_count")
        )
        .orderBy("month")
    )

    # ---------------------------------------------------------
    # 6. Data quality summary
    # ---------------------------------------------------------

    quality_df = (
        spark.read
        .parquet(quality_path)
    )

    data_quality_summary = (
        quality_df
        .agg(
            F.sum("total_records")
            .alias("total_records"),

            F.sum("missing_smart_records")
            .alias("missing_smart_records"),

            F.sum("schema_drift_records")
            .alias("schema_drift_records"),

            F.sum("affected_records")
            .alias("affected_records")
        )
        .withColumn(
            "affected_percentage",
            F.when(
                F.col("total_records") > 0,
                F.col("affected_records")
                / F.col("total_records")
                * 100
            ).otherwise(0)
        )
    )

    # ---------------------------------------------------------
    # Write analytics outputs
    # ---------------------------------------------------------

    (
        afr_by_model
        .write
        .mode("overwrite")
        .parquet(
            f"{output_path}/afr_by_model"
        )
    )

    (
        top_10_reliable
        .write
        .mode("overwrite")
        .parquet(
            f"{output_path}/top_10_reliable"
        )
    )

    (
        top_10_unreliable
        .write
        .mode("overwrite")
        .parquet(
            f"{output_path}/top_10_unreliable"
        )
    )

    (
        manufacturer_stats
        .write
        .mode("overwrite")
        .parquet(
            f"{output_path}/manufacturer_stats"
        )
    )

    (
        monthly_failure_trend
        .write
        .mode("overwrite")
        .parquet(
            f"{output_path}/monthly_failure_trend"
        )
    )

    (
        data_quality_summary
        .write
        .mode("overwrite")
        .parquet(
            f"{output_path}/data_quality_summary"
        )
    )
