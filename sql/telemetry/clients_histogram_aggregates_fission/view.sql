CREATE OR REPLACE VIEW `moz-fx-data-shared-prod.telemetry.clients_histogram_aggregates_fission` AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_histogram_aggregates_v1`
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry_derived.clients_has_fission_v1`
USING
  (submission_date, client_id, os, channel, app_version, app_build_id)
