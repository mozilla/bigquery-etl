-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `firefox_desktop.baseline_clients_first_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1`
