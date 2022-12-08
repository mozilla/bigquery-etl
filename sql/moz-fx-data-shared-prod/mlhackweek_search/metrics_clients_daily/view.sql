CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mlhackweek_search.metrics_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mlhackweek_search_derived.metrics_clients_daily_v1`
