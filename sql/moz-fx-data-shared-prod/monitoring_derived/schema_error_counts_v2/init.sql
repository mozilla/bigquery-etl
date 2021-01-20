CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.schema_error_counts_v2`(
    submission_date DATE,
    document_namespace STRING,
    document_type STRING,
    document_version STRING,
    hour TIMESTAMP,
    job_name STRING,
    path STRING,
    error_count INT64
  )
PARTITION BY
  submission_date
CLUSTER BY
  document_namespace,
  document_type,
  path,
  job_name
