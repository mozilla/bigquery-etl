CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_by_dlsource`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_by_dlsource_v1`
