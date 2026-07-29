CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glean_telemetry.rolling_cohorts`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.glean_telemetry_derived.rolling_cohorts_v1`
