CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.quicksuggest_click`
AS
SELECT
    * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata,
    LOWER(advertiser) AS advertiser
  )
FROM
  `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_click_v1`
