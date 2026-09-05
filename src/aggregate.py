from pyspark.sql import functions as F


def add_manufacturer(df):
    model = F.upper(F.trim(F.col("model")))

    return (
        df.withColumn(
            "manufacturer",
            F.when(
                model.rlike(r"^(ST|SEAGATE)"),
                F.lit("Seagate")
            )
            .when(
                model.rlike(r"^(WDC|WD)"),
                F.lit("WDC")
            )
            .when(
                model.rlike(r"^HGST"),
                F.lit("HGST")
            )
            .when(
                model.rlike(r"^(TOSHIBA|MG)"),
                F.lit("Toshiba")
            )
            .otherwise(F.lit("UNKNOWN"))
        )
    )


def build_daily_aggregate(df):
    df = add_manufacturer(df)

    daily_model_stats = (
        df
        .groupBy(
            "drive_date",
            "model",
            "manufacturer"
        )
        .agg(
            F.count("*").alias("drive_days"),
            F.sum(
                F.when(F.col("failure") == 1, 1).otherwise(0)
            ).alias("failure_count")
        )
    )

    return daily_model_stats
