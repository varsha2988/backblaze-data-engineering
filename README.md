# Backblaze Data Engineering Assessment

PySpark + AWS solution design for the Backblaze Drive Stats assessment.

## AWS Architecture
Backblaze daily CSVs -> Amazon S3 Raw -> AWS Glue PySpark -> S3 Curated/Parquet -> Glue Data Catalog -> Athena.

Supporting services: IAM and CloudWatch. Glue Workflows/EventBridge and QuickSight are optional.

## Requirements Addressed
- ~730 daily CSV files / 5GB+
- Peak memory <= 2GB
- Dynamic schema-drift handling
- Incremental processing
- Time/space complexity
- Null SMART values
- Drive appearance/disappearance
- Inconsistent capacity reporting
- AFR by model
- Top 10 reliable/least reliable models
- Manufacturer drive-days/failures
- Monthly failure trend
- Schema-drift/missing-SMART percentage

## Repository
```text
backblaze-data-engineering/
├── README.md
├── requirements.txt
├── download_data.sh
├── .gitignore
├── docs/
│   └── Backblaze_PySpark_AWS_Assessment_Documentation.docx
├── src/
│   ├── pipeline.py
│   ├── ingest.py
│   ├── schema_manager.py
│   ├── data_quality.py
│   ├── aggregate.py
│   └── analytics.py
├── sql/
│   └── analytics.sql
└── output/
    └── .gitkeep
```

## Processing Flow
1. Discover daily files in S3.
2. Compare files with processed-file control state.
3. Process only new files.
4. Read with Spark; never collect raw history to the driver.
5. Inspect schema dynamically.
6. Detect schema drift and maintain schema metadata.
7. Align stable fields to a canonical schema.
8. Preserve missing SMART values as NULL.
9. Flag schema drift/missing SMART records.
10. Profile capacity variants by model.
11. Do not interpret a missing daily drive as a failure.
12. Aggregate daily drive-days and failures.
13. Write Parquet aggregates.
14. Update control state after successful writes.
15. Run analytics from aggregates, not raw history.

## Incremental Control
Track `source_file`, `file_date`, `status`, `row_count`, `schema_hash`, and `processed_at`. A successful file is not counted twice.

## AFR
`AFR (%) = failure_count / drive_days * 365.25 * 100`

Top reliable = lowest AFR; least reliable = highest AFR. A minimum observation threshold may be applied to avoid tiny-sample bias.

## Complexity
For N records processed in a run, core scan/transformation/aggregation is approximately O(N) logical work, with distributed shuffle cost. Ranking M models is O(M log M). Space is bounded by Spark partitions/shuffle/control state rather than the complete raw history.

## Important
Raw Backblaze data must NOT be committed to Git. Use `download_data.sh` as the download entry point. If AWS is not deployed, describe the architecture as the target design rather than claiming deployment.
