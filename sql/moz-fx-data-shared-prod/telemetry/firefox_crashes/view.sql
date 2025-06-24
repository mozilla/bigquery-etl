CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_crashes`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_crashes_v1`
