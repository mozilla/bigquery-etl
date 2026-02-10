-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.baseline_clients_daily_v1`
WHERE
  is_new_profile
