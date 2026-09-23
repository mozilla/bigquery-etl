CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.quicksuggest_impression`
AS
SELECT
  * EXCEPT (search_query, matched_keywords) REPLACE(
    mozfun.norm.metadata(metadata) AS metadata,
    LOWER(advertiser) AS advertiser
  )
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1`
