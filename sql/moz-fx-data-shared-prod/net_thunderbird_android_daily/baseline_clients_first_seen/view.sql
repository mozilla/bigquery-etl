-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.net_thunderbird_android_daily.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.net_thunderbird_android_daily_derived.baseline_clients_daily_v1`
WHERE
  is_new_profile