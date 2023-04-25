CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigquery_usage_costs`
AS
SELECT
  creation_date,
  source_project,
  user_email,
  username,
  job_id,
  destination_table_id,
  SUM(total_terabytes_processed) * 4.15 AS cost_usd
FROM
  (
    SELECT
      creation_date,
      source_project,
      user_email,
      username,
      job_id,
      destination_table_id,
      total_terabytes_processed
    FROM
      `moz-fx-data-shared-prod.monitoring.bigquery_usage`
    WHERE
      state = 'DONE'
      AND job_type = 'QUERY'
      AND reservation_id IS NULL
      AND cache_hit IS FALSE
    GROUP BY
      creation_date,
      source_project,
      user_email,
      username,
      job_id,
      destination_table_id,
      total_terabytes_processed
  )
GROUP BY
  creation_date,
  source_project,
  user_email,
  username,
  job_id,
  destination_table_id
