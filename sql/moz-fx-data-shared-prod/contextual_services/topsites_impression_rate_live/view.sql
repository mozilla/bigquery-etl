CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.topsites_impression_rate_live`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.contextual_services_derived.topsites_impression_per_minute_v1` AS tc
