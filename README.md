# Backblaze Data Engineering Assessment

PySpark + AWS solution design for the Backblaze Hard Drive Test Data assessment.

## Overview

This project provides a scalable PySpark-based data engineering solution for processing two full years of Backblaze Hard Drive Test Data.

The solution is designed to handle:

- Approximately 730 daily CSV files
- 5GB+ of source data
- Peak memory constraint of 2GB
- Dynamic schema changes and SMART attribute drift
- Incremental processing
- Data-quality issues
- Model and manufacturer reliability analytics

Raw Backblaze data is intentionally not committed to this repository.

---

## AWS Architecture

The proposed AWS architecture is:

```text
Backblaze Daily CSV Files
          |
          v
      Amazon S3
       Raw Zone
          |
          v
   AWS Glue PySpark
          |
    +-----+-----+
    |           |
    v           v
S3 Aggregate  S3 Quality
   Parquet     Parquet
    |
    v
Glue Data Catalog
    |
    v
Amazon Athena
    |
    v
Analytics Results
