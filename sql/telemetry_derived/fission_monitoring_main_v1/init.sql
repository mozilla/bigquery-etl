CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.analysis.fission_monitoring_main_v1`( -- TODO: s/analysis/telemetry_derived/
    submission_date DATE,
    document_id STRING,
    sample_id INT64
  )
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
