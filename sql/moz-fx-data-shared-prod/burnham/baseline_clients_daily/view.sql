-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `burnham.baseline_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.burnham_derived.baseline_clients_daily_v1`
