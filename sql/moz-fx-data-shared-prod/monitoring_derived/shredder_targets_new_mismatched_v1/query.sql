WITH unmatching_sources AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.shredder_targets_joined_v1`
  WHERE
    matching_sources IS FALSE
    OR ARRAY_LENGTH(current_sources) = 0
),
previous_config AS (
  SELECT
    project_id,
    dataset_id,
    table_id,
  FROM
    unmatching_sources
  WHERE
    run_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
)
SELECT
  unmatching_sources.*,
FROM
  unmatching_sources
LEFT JOIN
  previous_config
  USING (project_id, dataset_id, table_id)
WHERE
  run_date = @submission_date
  AND previous_config.table_id IS NULL
