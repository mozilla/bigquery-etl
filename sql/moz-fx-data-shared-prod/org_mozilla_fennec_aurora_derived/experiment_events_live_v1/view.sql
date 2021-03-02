CREATE MATERIALIZED VIEW org_mozilla_fennec_aurora_derived.experiment_events_live_v1
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 5)
AS
WITH fenix_all_events AS (
  SELECT
    submission_timestamp,
    events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.events_v1`
),
fenix AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    extra1.value AS experiment,
    extra2.value AS branch,
    event.name AS event_method
  FROM
    fenix_all_events,
    UNNEST(events) AS event
  CROSS JOIN
    UNNEST(event.extra) AS extra1
  CROSS JOIN
    UNNEST(event.extra) AS extra2
  WHERE
    event.category = 'nimbus_events'
    AND extra1.key = 'experiment'
    AND extra2.key = 'branch'
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
  fenix
WHERE
  timestamp > TIMESTAMP('2021-03-01')
GROUP BY
  submission_date,
  `type`,
  experiment,
  branch,
  window_start,
  window_end
