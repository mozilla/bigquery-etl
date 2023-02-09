CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.bigquery_usage_costs`
AS
SELECT
  source_project,
  user_email,
  job_id,
  SUM(total_terabytes_processed) * 4.15 AS cost_usd
FROM
  (
    SELECT
      source_project,
      user_email,
      job_id,
      total_terabytes_processed
    FROM
      `moz-fx-data-shared-prod.monitoring.bigquery_usage`
    WHERE
      state = 'DONE'
      AND job_type = 'QUERY'
      AND reservation_id IS NULL
      AND cache_hit IS FALSE
    GROUP BY
      source_project,
      user_email,
      job_id,
      total_terabytes_processed
  )
GROUP BY
  source_project,
  user_email,
  job_id
