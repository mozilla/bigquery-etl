CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms.aggregated_search_terms_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.search_terms_derived.aggregated_search_terms_daily_v1`
