-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitor_cirrus.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.monitor_cirrus_derived.baseline_clients_daily_v1`
WHERE
  is_new_profile
