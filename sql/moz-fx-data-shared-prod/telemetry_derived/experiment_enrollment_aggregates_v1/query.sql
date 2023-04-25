-- Generated via ./bqetl generate experiment_monitoring
WITH org_mozilla_firefox_beta AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_fenix AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_firefox AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_firefox AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_firefoxbeta AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_fennec AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
desktop_all_events AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.event_events_v1`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.main_events_v1`
),
telemetry AS (
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
    AND submission_date = @submission_date
),
org_mozilla_klar AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_focus AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_focus_nightly AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_focus_beta AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_klar AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_focus AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    event.name AS event_method
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus.events`,
    UNNEST(events) AS event
  WHERE
    event.category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
all_events AS (
  SELECT
    *
  FROM
    org_mozilla_firefox_beta
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_fenix
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_firefox
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_firefox
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_firefoxbeta
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_fennec
  UNION ALL
  SELECT
    *
  FROM
    telemetry
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_klar
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus_nightly
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_focus_beta
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_klar
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_ios_focus
)
SELECT
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
  COUNTIF(event_method = 'expose' OR event_method = 'exposure') AS exposure_count,
  COUNTIF(event_method = 'validationFailed') AS validation_failed_count,
FROM
  all_events
GROUP BY
  `type`,
  experiment,
  branch,
  window_start,
  window_end
