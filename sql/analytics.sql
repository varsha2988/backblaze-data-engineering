-- ============================================================
-- Backblaze Hard Drive Analytics
-- SQL reference queries for AWS Athena / Spark SQL
-- ============================================================


-- ============================================================
-- 1. AFR by Model
-- ============================================================
-- Annualized Failure Rate (AFR):
-- AFR = (Failures / Drive-Days) * 365.25 * 100

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
-- 4. Drive-Days and Failures by Manufacturer
-- ============================================================
-- Manufacturer is created during the PySpark aggregation step.

SELECT
    manufacturer,
    SUM(drive_days) AS drive_days,
    SUM(failure_count) AS failure_count
FROM aggregate_data
GROUP BY manufacturer
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
-- 6. Overall Data Quality Summary
-- ============================================================

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
-- 7. Data Quality Trend by Date
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
