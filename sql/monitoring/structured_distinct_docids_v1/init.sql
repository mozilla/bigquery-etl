CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring.structured_distinct_docids_v1`(
    submission_date DATE,
    namespace STRING,
    doc_type STRING,
    decoded INT64,
    live INT64,
    stable INT64
  )
PARTITION BY
  submission_date
