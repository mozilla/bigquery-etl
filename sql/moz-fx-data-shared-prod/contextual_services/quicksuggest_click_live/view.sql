CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.quicksuggest_click_live`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.contextual_services_live.quicksuggest_click_v1`
