CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_retention_1_week`
AS
SELECT
  client_id,
  sample_id,
  DATE_SUB(submission_date, INTERVAL 13 DAY) AS `date`,
  -- active week 1
  `moz-fx-data-shared-prod`.udf.active_n_weeks_ago(days_seen_bits, 0) AS retained,
FROM
  `moz-fx-data-shared-prod.telemetry.clients_last_seen`
WHERE
  -- active week 0
  `moz-fx-data-shared-prod`.udf.active_n_weeks_ago(days_seen_bits, 1)
