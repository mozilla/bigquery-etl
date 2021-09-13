CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms.quicksuggest_impression`
AS
SELECT
  * REPLACE(mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
