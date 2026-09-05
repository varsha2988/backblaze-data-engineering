import hashlib

from pyspark.sql import functions as F


STABLE_FIELDS = {
    "date",
    "serial_number",
    "model",
    "capacity_bytes",
    "failure",
    "vault_id",
    "pod_id",
    "is_legacy_format",
    "datacenter",
    "cluster_id",
    "pod_slot_num",
}


def calculate_schema_hash(df):
    """
    Generate a deterministic hash for the incoming schema.
    """

    schema_string = "|".join(
        f"{field.name}:{field.dataType.simpleString()}"
        for field in df.schema.fields
    )

    return hashlib.sha256(
        schema_string.encode("utf-8")
    ).hexdigest()


def identify_smart_columns(df):
    """
    Identify SMART attributes dynamically.

    No hardcoded SMART attribute list is maintained.
    """

    return [
        column
        for column in df.columns
        if column not in STABLE_FIELDS
    ]


def detect_schema_drift(current_columns, previous_columns):
    """
    Compare current and previous schemas.
    """

    current = set(current_columns)
    previous = set(previous_columns)

    added = sorted(current - previous)
    removed = sorted(previous - current)

    return {
        "added_columns": added,
        "removed_columns": removed,
        "has_schema_drift": bool(added or removed)
    }


def prepare_daily_dataframe(spark, file_path):

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    required_columns = {
        "date",
        "serial_number",
        "model",
        "capacity_bytes",
        "failure"
    }

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {sorted(missing_columns)}"
        )

    schema_hash = calculate_schema_hash(df)

    smart_columns = identify_smart_columns(df)

    df = (
        df
        .withColumn(
            "drive_date",
            F.to_date(F.col("date"))
        )
        .withColumn(
            "failure",
            F.col("failure").cast("integer")
        )
        .withColumn(
            "capacity_bytes",
            F.col("capacity_bytes").cast("long")
        )
        .withColumn(
            "source_file",
            F.lit(file_path)
        )
        .withColumn(
            "schema_hash",
            F.lit(schema_hash)
        )
    )

    schema_metadata = {
        "columns": df.columns,
        "smart_columns": smart_columns,
        "schema_hash": schema_hash
    }

    return df, schema_metadata
