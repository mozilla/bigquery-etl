CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_enrollment_daily_active_population`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_daily_active_population_v1`
