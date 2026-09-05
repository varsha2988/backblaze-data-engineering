from pyspark.sql import functions as F


def add_manufacturer(df):
    """
    Derive manufacturer from the Backblaze model value.

    The source data does not provide a consistently usable
    manufacturer field across all years, so a deterministic
    model-prefix mapping is used.

    Unknown models are retained as UNKNOWN rather than
    being guessed.
    """

    model = F.upper(F.trim(F.col("model")))

    return (
        df.withColumn(
            "manufacturer",
            F.when(
                model.rlike(r"^(ST|SEAGATE)"),
                F.lit("Seagate")
            )
            .when(
                model.rlike(r"^(WDC|WD|HGST)"),
                F.lit("Western Digital")
            )
            .when(
                model.rlike(r"^(TOSHIBA|MG|DT)"),
                F.lit("Toshiba")
            )
            .otherwise(
                F.lit("UNKNOWN")
            )
        )
    )


def build_daily_aggregate(df):
    """
    Create daily model-level and manufacturer-level metrics.

    One valid daily drive observation represents one drive-day.

    failure_count is based only on the explicit Backblaze
    failure indicator. A drive disappearing from a later
    daily file is not automatically treated as a failure.
    """

    df = add_manufacturer(df)

    daily_model_stats = (
        df
        .groupBy(
            "drive_date",
            "model",
            "manufacturer"
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
