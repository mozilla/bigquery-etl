CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates_device`
AS
SELECT
  *,
  `mozfun.norm.os`(os) AS os_grouped
FROM
  `moz-fx-data-shared-prod.telemetry_derived.active_users_aggregates_device_v1`
