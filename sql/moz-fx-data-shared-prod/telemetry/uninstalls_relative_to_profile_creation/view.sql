CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_relative_to_profile_creation`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_relative_to_profile_creation_v1`
