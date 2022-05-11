CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.rolling_cohorts`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.rolling_cohorts_v1`
