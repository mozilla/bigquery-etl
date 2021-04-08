-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `mozphab.baseline_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mozphab_derived.baseline_clients_daily_v1`
