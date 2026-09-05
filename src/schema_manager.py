import hashlib
import re

from pyspark.sql import functions as F


# Columns that are expected to remain stable across files.
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
    Create a hash from column names and Spark data types.

    The hash helps identify schema changes between files.
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
    Dynamically identify SMART attribute columns.

    Example:
        smart_1_normalized
        smart_1_raw
        smart_5_normalized
        smart_5_raw

    No complete SMART column list is hardcoded.
    """

    smart_pattern = re.compile(
        r"^smart_\d+_(normalized|raw)$",
        re.IGNORECASE
    )

    return [
        column
        for column in df.columns
        if smart_pattern.match(column)
    ]


def detect_schema_drift(current_columns, previous_columns):
    """
    Compare current and previous column names.

    Returns:
        added_columns
        removed_columns
        has_schema_drift
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


def align_required_columns(df):
    """
    Validate and standardize the columns required by the pipeline.

    SMART columns are not required because they can be added,
    removed, or missing because of schema drift.
    """

    required_columns = {
        "date",
        "serial_number",
        "model",
        "capacity_bytes",
        "failure"
    }

    missing_columns = (
        required_columns - set(df.columns)
    )

    if missing_columns:
        raise ValueError(
            f"Missing required columns: "
            f"{sorted(missing_columns)}"
        )

    return (
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
    )


def prepare_daily_dataframe(spark, file_path):
    """
    Read one daily CSV and prepare it for processing.

    The file is processed independently so that the complete
    historical dataset does not need to be loaded into memory.
    """

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

    # Capture the original schema before adding pipeline columns.
    schema_hash = calculate_schema_hash(df)

    smart_columns = identify_smart_columns(df)

    df = align_required_columns(df)

    df = (
        df
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
        "original_columns": [
            field.name
            for field in df.schema.fields
            if field.name not in {
                "drive_date",
                "source_file",
                "schema_hash"
            }
        ],
        "smart_columns": smart_columns,
        "schema_hash": schema_hash
    }

    return df, schema_metadata
