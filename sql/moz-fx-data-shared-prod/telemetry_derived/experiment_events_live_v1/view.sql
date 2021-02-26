CREATE MATERIALIZED VIEW telemetry_derived.experiment_events_live_v1
OPTIONS (enable_refresh = true, refresh_interval_minutes = 5)
AS
WITH desktop_live AS (
  SELECT
    submission_timestamp AS timestamp,
    event.f2_ AS event_method,
    event.f3_ AS `type`,
    event.f4_ AS experiment,
    event_map_value.value AS branch
  FROM
    `moz-fx-data-shared-prod.telemetry_live.event_v4`
  CROSS JOIN
    UNNEST(payload.events.parent) AS event
  CROSS JOIN
    UNNEST(event.f5_) AS event_map_value
  WHERE event.f1_ = 'normandy' AND event_map_value.key = 'branch'
)
SELECT
  date(`timestamp`) AS submission_date,
  `type`,
  experiment,
  branch,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    -- Aggregates event counts over 5-minute intervals
    INTERVAL(DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
  ) AS window_start,
  TIMESTAMP_ADD(
    TIMESTAMP_TRUNC(`timestamp`, HOUR),
    INTERVAL((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
  ) AS window_end,
  COUNTIF(event_method = 'enroll' OR event_method = 'enrollment') AS enroll_count,
  COUNTIF(event_method = 'unenroll' OR event_method = 'unenrollment') AS unenroll_count,
  COUNTIF(event_method = 'graduate') AS graduate_count,
  COUNTIF(event_method = 'update') AS update_count,
  COUNTIF(event_method = 'enrollFailed') AS enroll_failed_count,
  COUNTIF(event_method = 'unenrollFailed') AS unenroll_failed_count,
  COUNTIF(event_method = 'updateFailed') AS update_failed_count,
  COUNTIF(event_method = 'disqualification') AS disqualification_count,
  COUNTIF(event_method = 'expose' OR event_method = 'exposure') AS exposure_count
FROM
  desktop_live
GROUP BY
  submission_date,
  `type`,
  experiment,
  branch,
  window_start,
  window_end

-- use CROSS JOIN unnest(array_concat(payload.events.content, payload.events.dynamic)) as x