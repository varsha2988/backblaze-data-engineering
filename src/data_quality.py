from pyspark.sql import functions as F


def build_quality_metrics(df, schema_metadata):

    smart_columns = schema_metadata["smart_columns"]

    # A record is considered to have missing SMART data
    # when all available SMART fields are NULL.
    if smart_columns:

        missing_smart_expression = None

        for column in smart_columns:

            expression = F.col(column).isNull()

            if missing_smart_expression is None:
                missing_smart_expression = expression
            else:
                missing_smart_expression = (
                    missing_smart_expression & expression
                )

    else:
        missing_smart_expression = F.lit(True)

    df = (
        df
        .withColumn(
            "missing_smart_flag",
            missing_smart_expression
        )
        .withColumn(
            "schema_drift_flag",
            F.lit(False)
        )
    )

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
            ).alias("missing_smart_records"),

            F.sum(
                F.when(
                    F.col("schema_drift_flag"),
                    1
                ).otherwise(0)
            ).alias("schema_drift_records"),

            F.sum(
                F.when(
                    F.col("schema_drift_flag")
                    | F.col("missing_smart_flag"),
                    1
                ).otherwise(0)
            ).alias("affected_records")
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

    return quality_df
