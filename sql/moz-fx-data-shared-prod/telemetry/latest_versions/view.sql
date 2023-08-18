CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.latest_versions`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.latest_versions_v2`
