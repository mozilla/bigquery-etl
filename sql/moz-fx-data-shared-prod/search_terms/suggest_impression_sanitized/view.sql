CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.search_terms.suggest_impression_sanitized`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v1`
