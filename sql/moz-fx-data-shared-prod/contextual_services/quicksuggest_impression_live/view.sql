CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.quicksuggest_impression_live`
AS
SELECT
  * EXCEPT (search_query, matched_keywords) REPLACE(mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.contextual_services_live.quicksuggest_impression_v1`
