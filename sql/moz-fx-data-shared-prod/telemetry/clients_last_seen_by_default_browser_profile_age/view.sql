CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_by_default_browser_profile_age`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_by_default_browser_profile_age_v1`
