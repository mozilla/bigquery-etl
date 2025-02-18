SELECT
  @submission_date AS submission_date,
  project_id,
  table_catalog,
  table_schema,
  table_name,
  creation_time,
  total_rows,
  total_partitions,
  total_logical_bytes,
  active_logical_bytes,
  long_term_logical_bytes,
  current_physical_bytes,
  total_physical_bytes,
  active_physical_bytes,
  long_term_physical_bytes,
  time_travel_physical_bytes,
  storage_last_modified_time,
  deleted,
  table_type,
  fail_safe_physical_bytes
FROM
  `moz-fx-data-shared-prod.region-US.INFORMATION_SCHEMA.TABLE_STORAGE`
