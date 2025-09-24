-- Generated via ./bqetl generate glean_usage
WITH base_firefox_desktop_data_leak_blocker_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.data_leak_blocker_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
          -- See https://mozilla-hub.atlassian.net/browse/DENG-9732
    AND (
      event.category = "uptake.remotecontent.result"
      AND event.name IN ("uptake_remotesettings", "uptake_normandy")
      AND mozfun.norm.extract_version(client_info.app_display_version, 'major') >= 143
      AND sample_id != 0
    ) IS NOT TRUE
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_newtab_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_profiles_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.profiles_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_prototype_no_code_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.prototype_no_code_events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_sync_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.sync_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_urlbar_keyword_exposure_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_keyword_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_urlbar_potential_exposure_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_potential_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
firefox_desktop_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Desktop" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_firefox_desktop_data_leak_blocker_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_newtab_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_profiles_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_prototype_no_code_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_sync_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_urlbar_keyword_exposure_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_urlbar_potential_exposure_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_crashreporter_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_crashreporter_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
firefox_crashreporter_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Crash Reporter" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_firefox_crashreporter_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_background_defaultagent_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
firefox_desktop_background_defaultagent_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Desktop Default Agent Task" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_firefox_desktop_background_defaultagent_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_pine_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.pine_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
pine_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Pinebuild" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_pine_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_home_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_firefox_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_firefox_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_home_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_beta_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_beta_home_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_beta_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_firefox_beta_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_firefox_beta_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_beta_home_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_beta_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_home_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_fenix_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_fenix_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_fenix_home_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_fenix_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_nightly_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_nightly_home_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_nightly_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_fenix_nightly_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_fenix_nightly_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_fenix_nightly_home_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_fenix_nightly_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fennec_aurora_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fennec_aurora_home_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fennec_aurora_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_fennec_aurora_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_fennec_aurora_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_fennec_aurora_home_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_fennec_aurora_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefox_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefox_first_session_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefox_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_firefox_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for iOS" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefox_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefox_first_session_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefox_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxbeta_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxbeta_first_session_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxbeta_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_firefoxbeta_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for iOS" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxbeta_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxbeta_first_session_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxbeta_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_fennec_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_fennec_first_session_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_fennec_metrics_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_fennec_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for iOS" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_ios_fennec_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_fennec_first_session_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_fennec_metrics_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_reference_browser_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_reference_browser_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Reference Browser" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_reference_browser_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_tv_firefox_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_tv_firefox_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Fire TV" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_tv_firefox_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_vrbrowser_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_vrbrowser_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_vrbrowser_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Reality" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_vrbrowser_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozilla_lockbox_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozilla_lockbox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
mozilla_lockbox_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Lockwise for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_mozilla_lockbox_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_lockbox_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_lockbox_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Lockwise for iOS" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_ios_lockbox_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_mozregression_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_mozregression_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "mozregression" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_mozregression_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_burnham_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.burnham_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
burnham_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Burnham" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_burnham_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozphab_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozphab_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
mozphab_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "mozphab" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_mozphab_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_connect_firefox_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_connect_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_connect_firefox_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox for Echo Show" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_connect_firefox_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefoxreality_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_firefoxreality_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Reality for PC-connected VR platforms" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_firefoxreality_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozilla_mach_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozilla_mach_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
mozilla_mach_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "mach" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_mozilla_mach_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_focus_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_focus_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Focus for iOS" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_ios_focus_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_klar_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_klar_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Klar for iOS" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_ios_klar_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_focus_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_focus_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Focus for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_focus_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_focus_beta_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_focus_beta_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Focus for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_focus_beta_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_focus_nightly_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_focus_nightly_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Focus for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_focus_nightly_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_klar_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_klar_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Klar for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_org_mozilla_klar_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_bergamot_custom_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.custom_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_bergamot_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_bergamot_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Bergamot Translator" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_bergamot_custom_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_bergamot_events_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_translations_custom_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_translations_stable.custom_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_translations_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_translations_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
firefox_translations_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Translations" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_firefox_translations_custom_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_translations_events_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_daemonsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_extensionsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_main_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_vpnsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
mozillavpn_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla VPN" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_mozillavpn_daemonsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_mozillavpn_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_mozillavpn_extensionsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_mozillavpn_main_v1
      UNION ALL
      SELECT
        *
      FROM
        base_mozillavpn_vpnsession_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_vpn_daemonsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_vpn_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_vpn_extensionsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_vpn_main_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_vpn_vpnsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_firefox_vpn_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla VPN" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_firefox_vpn_daemonsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_vpn_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_vpn_extensionsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_vpn_main_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_firefox_vpn_vpnsession_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_daemonsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_extensionsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_main_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_vpnsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_firefoxvpn_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla VPN" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_daemonsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_extensionsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_main_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_vpnsession_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_network_extension_daemonsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_network_extension_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_network_extension_extensionsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_network_extension_main_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_network_extension_vpnsession_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
org_mozilla_ios_firefoxvpn_network_extension_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla VPN" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_network_extension_daemonsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_network_extension_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_network_extension_extensionsession_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_network_extension_main_v1
      UNION ALL
      SELECT
        *
      FROM
        base_org_mozilla_ios_firefoxvpn_network_extension_vpnsession_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_backend_cirrus_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
mozillavpn_backend_cirrus_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla VPN Cirrus Sidecar" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_mozillavpn_backend_cirrus_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_glean_dictionary_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.glean_dictionary_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
glean_dictionary_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Glean Dictionary" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_glean_dictionary_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mdn_fred_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mdn_fred_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
mdn_fred_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "MDN" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_mdn_fred_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mdn_yari_action_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.action_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mdn_yari_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
mdn_yari_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "MDN (2022–2025)" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_mdn_yari_action_v1 UNION ALL SELECT * FROM base_mdn_yari_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_bedrock_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_bedrock_interaction_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.interaction_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_bedrock_non_interaction_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.non_interaction_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
bedrock_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "www.mozilla.org" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_bedrock_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_bedrock_interaction_v1
      UNION ALL
      SELECT
        *
      FROM
        base_bedrock_non_interaction_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_viu_politica_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_viu_politica_main_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.main_events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_viu_politica_video_index_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.video_index_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
viu_politica_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Viu Politica" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_viu_politica_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_viu_politica_main_events_v1
      UNION ALL
      SELECT
        *
      FROM
        base_viu_politica_video_index_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_treeherder_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.treeherder_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
treeherder_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Treeherder" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_treeherder_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_background_tasks_background_tasks_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.background_tasks_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_background_tasks_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
firefox_desktop_background_tasks_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Desktop background tasks" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (
      SELECT
        *
      FROM
        base_firefox_desktop_background_tasks_background_tasks_v1
      UNION ALL
      SELECT
        *
      FROM
        base_firefox_desktop_background_tasks_events_v1
    )
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_accounts_frontend_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.accounts_frontend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
accounts_frontend_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla Accounts Frontend" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_accounts_frontend_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_accounts_backend_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.accounts_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
accounts_backend_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla Accounts Backend" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_accounts_backend_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_accounts_cirrus_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.accounts_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
accounts_cirrus_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla Accounts (Cirrus)" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_accounts_cirrus_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_monitor_cirrus_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
monitor_cirrus_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla Monitor (Cirrus)" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_monitor_cirrus_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_debug_ping_view_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.debug_ping_view_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
debug_ping_view_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Glean Debug Ping Viewer" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_debug_ping_view_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_monitor_frontend_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.monitor_frontend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
monitor_frontend_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla Monitor (Frontend)" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_monitor_frontend_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_monitor_backend_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.monitor_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
monitor_backend_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Mozilla Monitor (Backend)" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_monitor_backend_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_relay_backend_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.relay_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
relay_backend_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Firefox Relay Backend" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_relay_backend_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_gleanjs_docs_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.gleanjs_docs_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
gleanjs_docs_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Glean.js Documentation" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_gleanjs_docs_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_thunderbird_desktop_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.thunderbird_desktop_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
thunderbird_desktop_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Thunderbird" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_thunderbird_desktop_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_net_thunderbird_android_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
net_thunderbird_android_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Thunderbird for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_net_thunderbird_android_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_net_thunderbird_android_beta_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
net_thunderbird_android_beta_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Thunderbird for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_net_thunderbird_android_beta_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_net_thunderbird_android_daily_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_daily_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
net_thunderbird_android_daily_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Thunderbird for Android" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_net_thunderbird_android_daily_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_syncstorage_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.syncstorage_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
syncstorage_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Sync Storage" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_syncstorage_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_glam_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.glam_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
glam_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "GLAM" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_glam_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_subscription_platform_backend_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.subscription_platform_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
subscription_platform_backend_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Subscription Platform" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_subscription_platform_backend_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_experimenter_cirrus_events_v1 AS (
  SELECT
    @submission_date AS submission_date,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
    COUNT(*) AS total_events,
  FROM
    `moz-fx-data-shared-prod.experimenter_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
        -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    channel,
    version,
    experiment,
    experiment_branch
),
experimenter_cirrus_aggregated AS (
  SELECT
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    "Experimenter (Cirrus)" AS normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch,
    SUM(total_events) AS total_events,
  FROM
    (SELECT * FROM base_experimenter_cirrus_events_v1)
  GROUP BY
    submission_date,
    window_start,
    window_end,
    event_category,
    event_name,
    event_extra_key,
    country,
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
)
SELECT
  *
FROM
  firefox_desktop_aggregated
UNION ALL
SELECT
  *
FROM
  firefox_crashreporter_aggregated
UNION ALL
SELECT
  *
FROM
  firefox_desktop_background_defaultagent_aggregated
UNION ALL
SELECT
  *
FROM
  pine_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_beta_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_fenix_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_fenix_nightly_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_fennec_aurora_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefox_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxbeta_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_fennec_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_reference_browser_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_tv_firefox_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_vrbrowser_aggregated
UNION ALL
SELECT
  *
FROM
  mozilla_lockbox_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_lockbox_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_mozregression_aggregated
UNION ALL
SELECT
  *
FROM
  burnham_aggregated
UNION ALL
SELECT
  *
FROM
  mozphab_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_connect_firefox_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefoxreality_aggregated
UNION ALL
SELECT
  *
FROM
  mozilla_mach_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_focus_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_klar_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_focus_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_focus_beta_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_focus_nightly_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_klar_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_bergamot_aggregated
UNION ALL
SELECT
  *
FROM
  firefox_translations_aggregated
UNION ALL
SELECT
  *
FROM
  mozillavpn_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_firefox_vpn_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_aggregated
UNION ALL
SELECT
  *
FROM
  org_mozilla_ios_firefoxvpn_network_extension_aggregated
UNION ALL
SELECT
  *
FROM
  mozillavpn_backend_cirrus_aggregated
UNION ALL
SELECT
  *
FROM
  glean_dictionary_aggregated
UNION ALL
SELECT
  *
FROM
  mdn_fred_aggregated
UNION ALL
SELECT
  *
FROM
  mdn_yari_aggregated
UNION ALL
SELECT
  *
FROM
  bedrock_aggregated
UNION ALL
SELECT
  *
FROM
  viu_politica_aggregated
UNION ALL
SELECT
  *
FROM
  treeherder_aggregated
UNION ALL
SELECT
  *
FROM
  firefox_desktop_background_tasks_aggregated
UNION ALL
SELECT
  *
FROM
  accounts_frontend_aggregated
UNION ALL
SELECT
  *
FROM
  accounts_backend_aggregated
UNION ALL
SELECT
  *
FROM
  accounts_cirrus_aggregated
UNION ALL
SELECT
  *
FROM
  monitor_cirrus_aggregated
UNION ALL
SELECT
  *
FROM
  debug_ping_view_aggregated
UNION ALL
SELECT
  *
FROM
  monitor_frontend_aggregated
UNION ALL
SELECT
  *
FROM
  monitor_backend_aggregated
UNION ALL
SELECT
  *
FROM
  relay_backend_aggregated
UNION ALL
SELECT
  *
FROM
  gleanjs_docs_aggregated
UNION ALL
SELECT
  *
FROM
  thunderbird_desktop_aggregated
UNION ALL
SELECT
  *
FROM
  net_thunderbird_android_aggregated
UNION ALL
SELECT
  *
FROM
  net_thunderbird_android_beta_aggregated
UNION ALL
SELECT
  *
FROM
  net_thunderbird_android_daily_aggregated
UNION ALL
SELECT
  *
FROM
  syncstorage_aggregated
UNION ALL
SELECT
  *
FROM
  glam_aggregated
UNION ALL
SELECT
  *
FROM
  subscription_platform_backend_aggregated
UNION ALL
SELECT
  *
FROM
  experimenter_cirrus_aggregated
