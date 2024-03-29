CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_unenrollment_overall_v1`
AS
SELECT
  window_start AS `time`,
  experiment,
  branch,
  SUM(`unenroll_count`) AS value
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live_v1`
GROUP BY
  1,
  2,
  3
