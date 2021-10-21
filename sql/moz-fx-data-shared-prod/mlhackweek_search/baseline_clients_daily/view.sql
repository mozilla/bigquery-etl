-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mlhackweek_search.baseline_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mlhackweek_search_derived.baseline_clients_daily_v1`
