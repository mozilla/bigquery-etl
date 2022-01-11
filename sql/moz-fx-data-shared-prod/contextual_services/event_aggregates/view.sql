CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.event_aggregates`
AS
SELECT
  * REPLACE (LOWER(advertiser) AS advertiser)
FROM
  `moz-fx-data-shared-prod.contextual_services_derived.event_aggregates_v1`
