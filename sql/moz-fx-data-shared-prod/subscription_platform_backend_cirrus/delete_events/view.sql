CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.subscription_platform_backend_cirrus.delete_events`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_backend_cirrus_derived.delete_events_v1`
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_backend_cirrus_derived.delete_events_v2`
