CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.structured_missing_columns_v1`(
    submission_date DATE,
    document_namespace STRING,
    document_type STRING,
    document_version STRING,
    path STRING,
    path_count INT64
  )
PARTITION BY
  submission_date
