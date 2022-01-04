CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.event_aggregates`
AS
SELECT
  * EXCEPT (advertiser),
  LOWER(advertiser) as advertiser
FROM
  `moz-fx-data-shared-prod.contextual_services_derived.event_aggregates_v1`
