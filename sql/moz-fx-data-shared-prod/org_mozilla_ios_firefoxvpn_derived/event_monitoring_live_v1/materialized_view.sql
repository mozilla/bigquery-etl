CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_derived.event_monitoring_live_v1`
  PARTITION BY
    DATE(submission_date)
  CLUSTER BY
    channel,
    event_category,
    event_name
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 60)
  AS
  WITH base_daemonsession_v1 AS (
    SELECT
      submission_timestamp,
      event.category AS event_category,
      event.name AS event_name,
      event_extra.key AS event_extra_key,
      normalized_country_code AS country,
      'Mozilla VPN' AS normalized_app_name,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
      COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
      COALESCE(
        ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
        '*'
      ) AS experiment_branch,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_live.daemonsession_v1`
    CROSS JOIN
      UNNEST(events) AS event
    CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
      UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
    LEFT JOIN
      UNNEST(event.extra) AS event_extra
  ),
  base_events_v1 AS (
    SELECT
      submission_timestamp,
      event.category AS event_category,
      event.name AS event_name,
      event_extra.key AS event_extra_key,
      normalized_country_code AS country,
      'Mozilla VPN' AS normalized_app_name,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
      COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
      COALESCE(
        ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
        '*'
      ) AS experiment_branch,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_live.events_v1`
    CROSS JOIN
      UNNEST(events) AS event
    CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
      UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
    LEFT JOIN
      UNNEST(event.extra) AS event_extra
  ),
  base_extensionsession_v1 AS (
    SELECT
      submission_timestamp,
      event.category AS event_category,
      event.name AS event_name,
      event_extra.key AS event_extra_key,
      normalized_country_code AS country,
      'Mozilla VPN' AS normalized_app_name,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
      COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
      COALESCE(
        ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
        '*'
      ) AS experiment_branch,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_live.extensionsession_v1`
    CROSS JOIN
      UNNEST(events) AS event
    CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
      UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
    LEFT JOIN
      UNNEST(event.extra) AS event_extra
  ),
  base_main_v1 AS (
    SELECT
      submission_timestamp,
      event.category AS event_category,
      event.name AS event_name,
      event_extra.key AS event_extra_key,
      normalized_country_code AS country,
      'Mozilla VPN' AS normalized_app_name,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
      COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
      COALESCE(
        ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
        '*'
      ) AS experiment_branch,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_live.main_v1`
    CROSS JOIN
      UNNEST(events) AS event
    CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
      UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
    LEFT JOIN
      UNNEST(event.extra) AS event_extra
  ),
  base_vpnsession_v1 AS (
    SELECT
      submission_timestamp,
      event.category AS event_category,
      event.name AS event_name,
      event_extra.key AS event_extra_key,
      normalized_country_code AS country,
      'Mozilla VPN' AS normalized_app_name,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
      COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
      COALESCE(
        ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
        '*'
      ) AS experiment_branch,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_live.vpnsession_v1`
    CROSS JOIN
      UNNEST(events) AS event
    CROSS JOIN
          -- Iterator for accessing experiments.
          -- Add one more for aggregating events across all experiments
      UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
    LEFT JOIN
      UNNEST(event.extra) AS event_extra
  ),
  combined AS (
    SELECT
      *
    FROM
      base_daemonsession_v1
    UNION ALL
    SELECT
      *
    FROM
      base_events_v1
    UNION ALL
    SELECT
      *
    FROM
      base_extensionsession_v1
    UNION ALL
    SELECT
      *
    FROM
      base_main_v1
    UNION ALL
    SELECT
      *
    FROM
      base_vpnsession_v1
  )
  SELECT
      -- used for partitioning, only allows TIMESTAMP columns
    TIMESTAMP_TRUNC(submission_timestamp, DAY) AS submission_date,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
        -- Aggregates event counts over 60-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 60) * 60) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 60) + 1) * 60) MINUTE
    ) AS window_end,
    * EXCEPT (submission_timestamp),
    COUNT(*) AS total_events,
  FROM
    combined
  WHERE
    DATE(submission_timestamp) >= "2025-04-18"
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
