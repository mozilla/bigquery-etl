SELECT
  * EXCEPT (submission_date)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_base`
WHERE
  window_start >= TIMESTAMP_SUB(@submission_timestamp, INTERVAL 1 HOUR)
  AND window_start < @submission_timestamp
  AND submission_date >= DATE(TIMESTAMP_SUB(@submission_timestamp, INTERVAL 2 HOUR))
