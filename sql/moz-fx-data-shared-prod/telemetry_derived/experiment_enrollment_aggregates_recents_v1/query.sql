SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_base`
WHERE
  timestamp > (
    SELECT
      MAX(timestamp)
    FROM
      `moz-fx-data-shared-prod.telemetry.experiment_enrollment_aggregates_hourly`
  )
