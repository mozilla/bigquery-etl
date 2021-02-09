CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_base`
AS
WITH desktop AS (
  SELECT
    timestamp,
    event_object AS `type`,
    event_string_value AS experiment,
    mozfun.map.get_key(event_map_values, 'branch') AS branch,
    event_method
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.events_live`
  WHERE
    event_category = 'normandy'
),
fenix_all_events AS (
  SELECT
    submission_timestamp,
    events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_live.events_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_live.events_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_live.events_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_live.events_v1`
  UNION ALL
  SELECT
    submission_timestamp,
    events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_live.events_v1`
),
fenix AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    fenix_all_events,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
),
all_events AS (
  SELECT
    *
  FROM
    desktop
  UNION ALL
  SELECT
    *
  FROM
    fenix
)
SELECT
  `timestamp`,
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
  all_events
GROUP BY
  timestamp,
  `type`,
  experiment,
  branch,
  window_start,
  window_end
