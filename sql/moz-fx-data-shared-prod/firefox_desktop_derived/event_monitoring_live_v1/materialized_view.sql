CREATE MATERIALIZED VIEW IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_desktop_derived.event_monitoring_live_v1`
PARTITION BY
  DATE(submission_date)
CLUSTER BY
  channel,
  event_category,
  event_name
OPTIONS
  (enable_refresh = FALSE, refresh_interval_minutes = 60)
AS
WITH base_data_leak_blocker_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.data_leak_blocker_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
          -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
),
base_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.events_v1`
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
            -- See https://mozilla-hub.atlassian.net/browse/DENG-9732
    (
      normalized_channel = 'release'
      AND event.category = 'uptake.remotecontent.result'
      AND event.name IN ('uptake_remotesettings', 'uptake_normandy')
      AND mozfun.norm.extract_version(client_info.app_display_version, 'major') >= 143
      AND sample_id != 0
    ) IS NOT TRUE
),
base_newtab_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.newtab_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
          -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
),
base_profiles_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.profiles_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
          -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
),
base_prototype_no_code_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.prototype_no_code_events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
          -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
),
base_sync_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.sync_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
          -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
),
base_urlbar_keyword_exposure_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.urlbar_keyword_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
          -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
),
base_urlbar_potential_exposure_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox for Desktop' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_live.urlbar_potential_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
          -- Add * extra to every event to get total event count
    UNNEST(event.extra ||[STRUCT<key STRING, value STRING>('*', NULL)]) AS event_extra
),
combined AS (
  SELECT
    *
  FROM
    base_data_leak_blocker_v1
  UNION ALL
  SELECT
    *
  FROM
    base_events_v1
  UNION ALL
  SELECT
    *
  FROM
    base_newtab_v1
  UNION ALL
  SELECT
    *
  FROM
    base_profiles_v1
  UNION ALL
  SELECT
    *
  FROM
    base_prototype_no_code_events_v1
  UNION ALL
  SELECT
    *
  FROM
    base_sync_v1
  UNION ALL
  SELECT
    *
  FROM
    base_urlbar_keyword_exposure_v1
  UNION ALL
  SELECT
    *
  FROM
    base_urlbar_potential_exposure_v1
)
SELECT
      -- used for partitioning, only allows TIMESTAMP columns
  TIMESTAMP_TRUNC(submission_timestamp, DAY) AS submission_date,
  TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS window_start,
  TIMESTAMP_ADD(TIMESTAMP_TRUNC(submission_timestamp, HOUR), INTERVAL 1 HOUR) AS window_end,
  * EXCEPT (submission_timestamp),
  COUNT(*) AS total_events,
FROM
  combined
WHERE
  DATE(submission_timestamp) >= "2025-10-30"
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
