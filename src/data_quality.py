from pyspark.sql import functions as F


def build_quality_metrics(df, schema_metadata):
    """
    Build data-quality metrics for the current daily file.

    Quality checks:
    1. Missing SMART data
    2. Schema drift
    3. Capacity inconsistency within a model
    """

    smart_columns = schema_metadata.get("smart_columns", [])

    # ---------------------------------------------------------
    # 1. Missing SMART data
    # ---------------------------------------------------------
    # A record is considered to have missing SMART data when
    # all available SMART attributes are NULL.
    #
    # If the file contains no SMART columns at all, every
    # record is considered affected.

    if smart_columns:

        missing_smart_expression = F.lit(True)

        for column in smart_columns:
            missing_smart_expression = (
                missing_smart_expression
                & F.col(column).isNull()
            )

    else:
        missing_smart_expression = F.lit(True)

    df = df.withColumn(
        "missing_smart_flag",
        missing_smart_expression
    )

    # ---------------------------------------------------------
    # 2. Schema drift
    # ---------------------------------------------------------
    # The pipeline can provide this value after comparing the
    # current schema with the schema registry.

    schema_drift_flag = schema_metadata.get(
        "has_schema_drift",
        False
    )

    df = df.withColumn(
        "schema_drift_flag",
        F.lit(bool(schema_drift_flag))
    )

    # ---------------------------------------------------------
    # 3. Capacity inconsistency
    # ---------------------------------------------------------
    # A model having more than one reported capacity in the
    # same daily file is flagged for data-quality review.
    #
    # We retain the original capacity value rather than
    # silently replacing it.

    capacity_counts = (
        df
        .filter(F.col("capacity_bytes").isNotNull())
        .groupBy("model")
        .agg(
            F.countDistinct("capacity_bytes")
            .alias("capacity_variants")
        )
    )

    df = (
        df
        .join(
            capacity_counts,
            on="model",
            how="left"
        )
        .withColumn(
            "capacity_inconsistent_flag",
            F.col("capacity_variants") > 1
        )
    )

    # ---------------------------------------------------------
    # 4. Daily quality summary
    # ---------------------------------------------------------

    quality_df = (
        df
        .groupBy("drive_date")
        .agg(
            F.count("*").alias("total_records"),

            F.sum(
                F.when(
                    F.col("missing_smart_flag"),
                    1
                ).otherwise(0)
            ).alias(
                "missing_smart_records"
            ),

            F.sum(
                F.when(
                    F.col("schema_drift_flag"),
                    1
                ).otherwise(0)
            ).alias(
                "schema_drift_records"
            ),

            F.sum(
                F.when(
                    F.col("capacity_inconsistent_flag"),
                    1
                ).otherwise(0)
            ).alias(
                "capacity_inconsistent_records"
            ),

            F.sum(
                F.when(
                    F.col("schema_drift_flag")
                    | F.col("missing_smart_flag"),
                    1
                ).otherwise(0)
            ).alias(
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
            ).otherwise(0)
        )
    )

    return quality_df
