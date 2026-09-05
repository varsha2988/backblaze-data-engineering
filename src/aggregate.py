from pyspark.sql import functions as F


def build_daily_aggregate(df):

    """
    One daily drive observation = one drive-day.

    Failure is counted only when the Backblaze failure
    field equals 1.
    """

    daily_model_stats = (
        df
        .groupBy(
            "drive_date",
            "model"
        )
        .agg(
            F.count("*").alias(
                "drive_days"
            ),

            F.sum(
                F.when(
                    F.col("failure") == 1,
                    1
                ).otherwise(0)
            ).alias(
                "failure_count"
            )
        )
    )

    return daily_model_stats
