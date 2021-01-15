SELECT
  window_start AS `time`,
  experiment,
  branch,
  sum(`unenroll_count`) AS value
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live`
GROUP BY
  1,
  2,
  3
