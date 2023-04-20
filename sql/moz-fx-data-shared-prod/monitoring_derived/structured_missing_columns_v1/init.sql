CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.monitoring_derived.structured_missing_columns_v1`(
    submission_date DATE,
    document_namespace STRING,
    document_type STRING,
    document_version STRING,
    path STRING,
    path_count INT64,
    formatted_path_string STRING,
    column_name_in_table STRING,
    missing_column_added BOOL
  )
PARTITION BY
  submission_date
