SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_base`
WHERE
  -- limit the range that is queries from the main live table to up to max 2 hours
  -- search aggregates hourly gets updated every 1 hour with a 30 minute delay
  timestamp > TIMESTAMP_SUB(@submission_timestamp, INTERVAL 2 HOUR)
  AND timestamp > (
    SELECT
      MAX(timestamp)
    FROM
      `moz-fx-data-shared-prod.telemetry.experiment_search_aggregates_hourly`
  )
