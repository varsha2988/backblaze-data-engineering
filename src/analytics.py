from pyspark.sql import functions as F


def write_analytics(
    spark,
    aggregate_path,
    quality_path,
    output_path
):

    aggregate_df = (
        spark.read
        .parquet(aggregate_path)
    )

    # -------------------------------------------------
    # 1. AFR BY MODEL
    # -------------------------------------------------

    afr_by_model = (
        aggregate_df
        .groupBy("model")
        .agg(
            F.sum("drive_days").alias(
                "drive_days"
            ),

            F.sum("failure_count").alias(
                "failure_count"
            )
        )
        .withColumn(
            "afr_percent",
            F.when(
                F.col("drive_days") > 0,
                F.col("failure_count")
                / F.col("drive_days")
                * 365.25
                * 100
            )
        )
    )

    # -------------------------------------------------
    # 2. TOP 10 MOST RELIABLE
    # -------------------------------------------------

    top_10_reliable = (
        afr_by_model
        .orderBy(
            F.col("afr_percent").asc()
        )
        .limit(10)
    )

    # -------------------------------------------------
    # 3. TOP 10 LEAST RELIABLE
    # -------------------------------------------------

    top_10_unreliable = (
        afr_by_model
        .orderBy(
            F.col("afr_percent").desc()
        )
        .limit(10)
    )

    # -------------------------------------------------
    # 4. MONTHLY FAILURE TREND
    # -------------------------------------------------

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
            F.sum("drive_days").alias(
                "drive_days"
            ),

            F.sum("failure_count").alias(
                "failure_count"
            )
        )
        .orderBy("month")
    )

    # -------------------------------------------------
    # 5. DATA QUALITY
    # -------------------------------------------------

    quality_df = (
        spark.read
        .parquet(quality_path)
    )

    data_quality_summary = (
        quality_df
        .agg(
            F.sum("total_records").alias(
                "total_records"
            ),

            F.sum("missing_smart_records").alias(
                "missing_smart_records"
            ),

            F.sum("schema_drift_records").alias(
                "schema_drift_records"
            ),

            F.sum("affected_records").alias(
                "affected_records"
            )
        )
        .withColumn(
            "affected_percentage",
            F.when(
                F.col("total_records") > 0,
                F.col("affected_records")
                / F.col("total_records")
                * 100
            )
        )
    )

    # -------------------------------------------------
    # WRITE OUTPUTS
    # -------------------------------------------------

    afr_by_model.write.mode(
        "overwrite"
    ).parquet(
        f"{output_path}/afr_by_model"
    )

    top_10_reliable.write.mode(
        "overwrite"
    ).parquet(
        f"{output_path}/top_10_reliable"
    )

    top_10_unreliable.write.mode(
        "overwrite"
    ).parquet(
        f"{output_path}/top_10_unreliable"
    )

    monthly_failure_trend.write.mode(
        "overwrite"
    ).parquet(
        f"{output_path}/monthly_failure_trend"
    )

    data_quality_summary.write.mode(
        "overwrite"
    ).parquet(
        f"{output_path}/data_quality_summary"
    )
