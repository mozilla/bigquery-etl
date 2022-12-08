CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mlhackweek_search.metrics_clients_last_seen`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mlhackweek_search_derived.metrics_clients_last_seen_v1`
