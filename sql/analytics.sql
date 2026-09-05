-- ============================================================
-- Backblaze Hard Drive Analytics
-- SQL reference queries for AWS Athena / Spark SQL
-- ============================================================

-- ============================================================
-- 1. Annualized Failure Rate (AFR) by Model
-- ============================================================
-- AFR = failures / drive-days * 365.25 * 100

SELECT
    model,
    SUM(drive_days) AS drive_days,
    SUM(failure_count) AS failure_count,
    ROUND(
        SUM(failure_count) / NULLIF(SUM(drive_days), 0)
        * 365.25 * 100,
        4
    ) AS afr_percent
FROM aggregate_data
GROUP BY model
ORDER BY afr_percent ASC;


-- ============================================================
-- 2. Top 10 Most Reliable Models
-- ============================================================

SELECT
    model,
    drive_days,
    failure_count,
    afr_percent
FROM afr_by_model
ORDER BY afr_percent ASC
LIMIT 10;


-- ============================================================
-- 3. Top 10 Least Reliable Models
-- ============================================================

SELECT
    model,
    drive_days,
    failure_count,
    afr_percent
FROM afr_by_model
ORDER BY afr_percent DESC
LIMIT 10;


-- ============================================================
-- 4. Drive-days and Failures by Manufacturer
-- ============================================================
-- Manufacturer is derived from the first token of the model name.
-- This is a practical proxy when a separate manufacturer field
-- is not available in the source data.

SELECT
    regexp_extract(model, '^([^ ]+)', 1) AS manufacturer,
    SUM(drive_days) AS drive_days,
    SUM(failure_count) AS failure_count
FROM aggregate_data
GROUP BY regexp_extract(model, '^([^ ]+)', 1)
ORDER BY failure_count DESC;


-- ============================================================
-- 5. Monthly Failure Trend
-- ============================================================

SELECT
    date_format(drive_date, 'yyyy-MM') AS month,
    SUM(drive_days) AS drive_days,
    SUM(failure_count) AS failure_count
FROM aggregate_data
GROUP BY date_format(drive_date, 'yyyy-MM')
ORDER BY month;


-- ============================================================
-- 6. Data Quality Summary
-- ============================================================
-- affected_percentage represents records affected by either
-- missing SMART data or schema drift.

SELECT
    SUM(total_records) AS total_records,
    SUM(missing_smart_records) AS missing_smart_records,
    SUM(schema_drift_records) AS schema_drift_records,
    SUM(affected_records) AS affected_records,
    ROUND(
        SUM(affected_records) / NULLIF(SUM(total_records), 0) * 100,
        4
    ) AS affected_percentage
FROM quality_data;


-- ============================================================
-- 7. Monthly Data Quality Trend
-- ============================================================

SELECT
    drive_date,
    total_records,
    missing_smart_records,
    schema_drift_records,
    affected_records,
    affected_percentage
FROM quality_data
ORDER BY drive_date;
