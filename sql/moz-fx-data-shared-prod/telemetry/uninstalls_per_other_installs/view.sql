CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_per_other_installs`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_per_other_installs_v1`
