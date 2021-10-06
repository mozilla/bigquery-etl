CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.contextual_services.topsites_click_rate_live`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.contextual_services_derived.topsites_click_per_minute_v1` AS tc
