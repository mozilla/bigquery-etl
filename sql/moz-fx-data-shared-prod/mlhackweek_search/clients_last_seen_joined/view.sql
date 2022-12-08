CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mlhackweek_search.clients_last_seen_joined`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.mlhackweek_search_derived.clients_last_seen_joined_v1`
