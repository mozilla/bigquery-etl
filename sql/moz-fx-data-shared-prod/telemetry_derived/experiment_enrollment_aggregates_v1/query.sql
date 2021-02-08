WITH desktop_all_events AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.event_events_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.main_events_v1`
), desktop AS (
  SELECT
    event_object AS `type`,
    event_string_value AS experiment,
    udf.get_key(event_map_values, 'branch') AS branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(`timestamp`, HOUR),
      -- Aggregates event counts over 5-minute intervals
      INTERVAL (DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(`timestamp`, HOUR),
      INTERVAL ((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
    ) AS window_end,
    COUNTIF(event_method = 'enroll') AS enroll_count,
    COUNTIF(event_method = 'unenroll') AS unenroll_count,
    COUNTIF(event_method = 'graduate') AS graduate_count,
    COUNTIF(event_method = 'update') AS update_count,
    COUNTIF(event_method = 'enrollFailed') AS enroll_failed_count,
    COUNTIF(event_method = 'unenrollFailed') AS unenroll_failed_count,
    COUNTIF(event_method = 'updateFailed') AS update_failed_count,
    0 AS disqualification_count,
    0 AS exposure_count
  FROM
    desktop_all_events
  WHERE
    submission_date = @submission_date
    AND event_category = 'normandy'
  GROUP BY
    `type`,
    experiment,
    branch,
    window_start,
    window_end
), fenix AS (
  SELECT
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      -- Aggregates event counts over 5-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) * 5) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 5) + 1) * 5) MINUTE
    ) AS window_end,
    COUNTIF(event.name = 'enrollment') AS enroll_count,
    COUNTIF(event.name = 'unenrollment') AS unenroll_count,
    0 AS graduate_count,
    0 AS update_count,
    0 AS enroll_failed_count,
    0 AS unenroll_failed_count,
    0 AS update_failed_count,
    COUNTIF(event.name = 'disqualification') AS disqualification_count,
    COUNTIF(event.name = 'exposure') AS exposure_count
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
  GROUP BY
    timestamp,
    `type`,
    experiment,
    branch,
    window_start,
    window_end
)
SELECT
  *
FROM
  desktop
UNION ALL
SELECT
  *
FROM
  fenix
