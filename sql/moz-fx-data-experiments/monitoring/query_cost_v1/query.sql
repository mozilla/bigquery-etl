SELECT
  creation_time AS submission_timestamp,
  destination_table.table_id AS destination_table,
  REPLACE(
    REGEXP_REPLACE(
      destination_table.table_id,
      r'_enrollments_[^_]+_[0-9]+$|_exposures_[^_]+_[0-9]+$|^enrollments_|^statistics_',
      ''
    ),
    '_',
    '-'
  ) AS experiment_slug,
  query,
  total_bytes_processed,
  total_slot_ms,
  total_slot_ms / 1000 / 60 / 60 * 0.06 AS cost_usd
FROM
  `moz-fx-data-experiments.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  DATE(creation_time) = '2024-01-31'
  AND destination_table.dataset_id = "mozanalysis"
  AND total_slot_ms > 3600 -- only costs > 0.001 USD
