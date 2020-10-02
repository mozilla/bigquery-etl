CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.fission_monitoring_main_v1`(
    submission_timestamp TIMESTAMP,
    document_id STRING,
    sample_id INT64
  )
PARTITION BY
  DATE(submission_timestamp)
CLUSTER BY
  sample_id
