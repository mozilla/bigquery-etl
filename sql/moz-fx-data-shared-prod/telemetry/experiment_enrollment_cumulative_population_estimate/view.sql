CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.experiment_enrollment_cumulative_population_estimate`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_cumulative_population_estimate_v1`
