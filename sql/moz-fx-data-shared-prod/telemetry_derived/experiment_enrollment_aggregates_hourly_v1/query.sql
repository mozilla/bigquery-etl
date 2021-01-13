SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_base`
WHERE
  timestamp >= TIMESTAMP_SUB(@submission_timestamp, INTERVAL 1 HOUR)
  AND timestamp < @submission_timestamp
