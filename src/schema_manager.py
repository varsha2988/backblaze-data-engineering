import hashlib
import re

from pyspark.sql import functions as F


# Stable business columns required by the assessment.
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
    Create a deterministic hash of the incoming schema.

    The hash is stored with the processed data so that
    different source schemas can be identified.
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

    Backblaze SMART columns follow patterns such as:
        smart_1_normalized
        smart_1_raw
        smart_5_normalized
        smart_5_raw

    We detect them using a regular expression instead of
    maintaining a hardcoded list of SMART column names.
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
    Compare the current file schema with the previous schema.

    Returns added and removed columns.

    A type change can be detected separately using the
    schema hash.
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
    Validate required business columns and normalize
    their data types.
    """

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
    Read and prepare one daily Backblaze CSV.

    Only one source file is processed at a time, helping
    maintain the assessment's memory constraint.
    """

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(file_path)
    )

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
        "smart_columns": smart_columns,
        "schema_hash": schema_hash
    }

    return df, schema_metadata
