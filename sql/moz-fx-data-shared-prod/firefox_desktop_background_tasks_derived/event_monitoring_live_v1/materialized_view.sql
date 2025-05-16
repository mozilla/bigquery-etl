CREATE MATERIALIZED VIEW IF NOT EXISTS
  `moz-fx-data-shared-prod.firefox_desktop_background_tasks_derived.event_monitoring_live_v1`
PARTITION BY
  DATE(submission_date)
CLUSTER BY
  channel,
  event_category,
  event_name
OPTIONS
  (enable_refresh = TRUE, refresh_interval_minutes = 60)
AS
WITH base_background_tasks_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox Desktop background tasks' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_live.background_tasks_v1`
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
    'Firefox Desktop background tasks' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
          -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_live.events_v1`
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
    base_background_tasks_v1
  UNION ALL
  SELECT
    *
  FROM
    base_events_v1
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
  DATE(submission_timestamp) >= "2025-05-16"
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
