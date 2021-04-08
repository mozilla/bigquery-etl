-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `firefox_desktop.baseline_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_daily_v1`
