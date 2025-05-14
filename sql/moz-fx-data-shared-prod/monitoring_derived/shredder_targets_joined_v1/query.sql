WITH query_counts AS (
  SELECT
    COUNT(*) AS query_count,
    targets.project_id,
    targets.dataset_id,
    targets.table_id,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1` AS jobs
  CROSS JOIN
    UNNEST(referenced_tables) AS ref_table
  RIGHT JOIN
    `moz-fx-data-shared-prod.monitoring_derived.shredder_targets_v1` AS targets
    ON targets.project_id = ref_table.project_id
    AND targets.dataset_id = ref_table.dataset_id
    AND STARTS_WITH(ref_table.table_id, targets.table_id)
    -- filter out shredder reads
    AND jobs.project_id NOT IN ('moz-fx-data-shredder', 'moz-fx-data-bq-batch-prod')
  WHERE
    DATE(creation_time)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 29 DAY)
    AND DATE(@submission_date)
    AND targets.run_date = @submission_date
  GROUP BY
    targets.project_id,
    targets.dataset_id,
    targets.table_id
),
write_counts AS (
  SELECT
    COUNT(*) AS write_count,
    targets.project_id,
    targets.dataset_id,
    targets.table_id,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.shredder_targets_v1` AS targets
  LEFT JOIN
    `moz-fx-data-shared-prod.monitoring_derived.jobs_by_organization_v1` AS jobs
    ON targets.project_id = destination_table.project_id
    AND targets.dataset_id = destination_table.dataset_id
    AND STARTS_WITH(destination_table.table_id, targets.table_id)
    -- filter out shredder writes
    AND jobs.project_id NOT IN ('moz-fx-data-shredder', 'moz-fx-data-bq-batch-prod')
  WHERE
    DATE(creation_time)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 29 DAY)
    AND DATE(@submission_date)
    AND targets.run_date = @submission_date
  GROUP BY
    targets.project_id,
    targets.dataset_id,
    targets.table_id
)
SELECT
  targets.*,
  owners,
  COALESCE(query_count, 0) AS query_count_last_30d,
  COALESCE(write_count, 0) AS write_count_last_30d,
  ROUND(total_logical_bytes / 1024 / 1024 / 1024 / 1024, 2) AS table_size_tib,
  table_inventory.creation_date AS table_creation_date,
  IFNULL(table_inventory.deprecated, FALSE) AS deprecated,
FROM
  `moz-fx-data-shared-prod.monitoring_derived.shredder_targets_v1` AS targets
LEFT JOIN
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_tables_inventory_v1` AS table_inventory
  USING (project_id, dataset_id, table_id)
LEFT JOIN
  `moz-fx-data-shared-prod.monitoring_derived.bigquery_table_storage_v1` AS table_storage
  USING (project_id, dataset_id, table_id)
LEFT JOIN
  query_counts
  USING (project_id, dataset_id, table_id)
LEFT JOIN
  write_counts
  USING (project_id, dataset_id, table_id)
WHERE
  targets.run_date = @submission_date
  AND table_inventory.submission_date = @submission_date
