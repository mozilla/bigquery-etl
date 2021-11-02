CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.quicksuggest_impression`
AS
SELECT
  * EXCEPT (search_query, matched_keywords) REPLACE(mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v1`
