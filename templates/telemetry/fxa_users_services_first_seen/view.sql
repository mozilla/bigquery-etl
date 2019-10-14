CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fxa_users_services_first_seen` AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fxa_users_services_first_seen_v1`
