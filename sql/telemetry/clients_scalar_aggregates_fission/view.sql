CREATE OR REPLACE VIEW `moz-fx-data-shared-prod.telemetry_derived.clients_scalar_aggregates_fission` AS
SELECT
  * EXCEPT (has_fission),
  COALESCE(has_fission, 1) AS has_fission,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_scalar_aggregates_v1`
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry_derived.clients_has_fission_v1`
USING
  (client_id, os, channel, app_version, app_build_id)
