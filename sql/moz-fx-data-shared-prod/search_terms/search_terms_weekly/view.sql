CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms.search_terms_weekly`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.search_terms_derived.search_terms_weekly_v1`
