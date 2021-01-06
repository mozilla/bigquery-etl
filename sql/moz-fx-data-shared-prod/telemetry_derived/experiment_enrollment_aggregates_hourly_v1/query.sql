SELECT
  timestamp,
  event_object AS `type`,
  event_string_value AS experiment,
  mozfun.map.get_key(event_map_values, 'branch') AS branch,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    -- Aggregates event counts over 5-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
  ) AS window_start,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    INTERVAL((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
  ) AS window_end,
  COUNTIF(event_method = 'enroll') AS enroll_count,
  COUNTIF(event_method = 'unenroll') AS unenroll_count,
  COUNTIF(event_method = 'graduate') AS graduate_count,
  COUNTIF(event_method = 'update') AS update_count,
  COUNTIF(event_method = 'enrollFailed') AS enroll_failed_count,
  COUNTIF(event_method = 'unenrollFailed') AS unenroll_failed_count,
  COUNTIF(event_method = 'updateFailed') AS update_failed_count
FROM
  `moz-fx-data-shared-prod.telemetry_derived.events_live`
WHERE
  event_category = 'normandy'
  AND timestamp >= TIME_SUB(@submission_timestamp, INTERVAL 1 HOUR)
  AND timestamp < @submission_timestamp
GROUP BY
  `type`,
  experiment,
  branch,
  window_start,
  window_end
