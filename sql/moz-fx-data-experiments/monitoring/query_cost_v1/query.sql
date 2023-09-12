SELECT
  creation_time AS submission_timestamp,
  destination_table.table_id AS destination_table,
  query,
  total_bytes_processed,
  total_bytes_processed / 1024 / 1024 / 1024 / 1024 * 5 AS cost_usd
FROM
  `moz-fx-data-experiments.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  DATE(creation_time) = @submission_date
  AND destination_table.dataset_id = "mozanalysis"
  AND total_bytes_processed IS NOT NULL
