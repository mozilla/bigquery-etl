CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms.search_queries_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.search_terms_derived.search_queries_daily_v1`
