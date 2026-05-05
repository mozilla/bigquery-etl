CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.uninstalls_by_cpu_cores`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.uninstalls_by_cpu_cores_v1`
