-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_js_tmp.baseline_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_js_tmp_derived.baseline_clients_daily_v1`
