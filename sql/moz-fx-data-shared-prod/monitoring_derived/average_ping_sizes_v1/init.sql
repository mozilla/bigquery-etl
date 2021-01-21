CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.average_ping_sizes_v1`(
    submission_date DATE,
    dataset_id STRING,
    table_id STRING,
    average_byte_size FLOAT64,
    total_byte_size INT64,
    row_count INT64
  )
PARTITION BY
  submission_date
