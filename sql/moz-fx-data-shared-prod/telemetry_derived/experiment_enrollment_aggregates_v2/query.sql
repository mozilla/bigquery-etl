-- Generated via ./bqetl generate experiment_monitoring
WITH org_mozilla_firefox_beta AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_fenix AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_firefox AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_firefox AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_firefoxbeta AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_fennec AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
firefox_desktop AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_klar AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_focus AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_focus_nightly AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_focus_beta AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_klar AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
org_mozilla_ios_focus AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event_category AS `type`,
    JSON_VALUE(event_extra, '$.experiment') AS experiment,
    JSON_VALUE(event_extra, '$.branch') AS branch,
    normalized_channel,
    event_name AS event_method,
      -- Before version 109 (in desktop), clients evaluated schema
      -- before targeting, so validation_errors are invalid
    IF(
      app_version_major >= 109
      OR normalized_app_name != 'firefox_desktop',
      TRUE,
      FALSE
    ) AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus.events_stream`
  WHERE
    event_category = 'nimbus_events'
    AND DATE(submission_timestamp) = @submission_date
),
monitor_cirrus AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    normalized_channel,
    event.name AS event_method,
    TRUE AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus.enrollment`,
    UNNEST(events) AS event
  WHERE
    event.category = 'cirrus_events'
    AND DATE(submission_timestamp) = @submission_date
),
accounts_cirrus AS (
  SELECT
    submission_timestamp AS `timestamp`,
    event.category AS `type`,
    mozfun.map.get_key(event.extra, 'experiment') AS experiment,
    mozfun.map.get_key(event.extra, 'branch') AS branch,
    normalized_channel,
    event.name AS event_method,
    TRUE AS validation_errors_valid
  FROM
    `moz-fx-data-shared-prod.accounts_cirrus.enrollment`,
    UNNEST(events) AS event
  WHERE
    event.category = 'cirrus_events'
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
    firefox_desktop
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
  UNION ALL
  SELECT
    *
  FROM
    monitor_cirrus
  UNION ALL
  SELECT
    *
  FROM
    accounts_cirrus
)
SELECT
  `type`,
  experiment,
  branch,
  normalized_channel,
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
  COUNTIF(event_method = 'validationFailed' AND validation_errors_valid) AS validation_failed_count,
FROM
  all_events
GROUP BY
  `type`,
  experiment,
  branch,
  normalized_channel,
  window_start,
  window_end
