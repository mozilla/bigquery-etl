CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.fission_monitoring_main_v1`(
    submission_date DATE,
    document_id STRING,
    sample_id INT64
  )
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
