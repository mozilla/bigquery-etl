CREATE MATERIALIZED VIEW
IF
  NOT EXISTS `moz-fx-data-shared-prod.org_mozilla_focus_nightly_derived.event_monitoring_live_v1`
  OPTIONS
    (enable_refresh = TRUE, refresh_interval_minutes = 60)
  AS
  SELECT
    DATE(submission_timestamp) AS submission_date,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
        -- Aggregates event counts over 60-minute intervals
      INTERVAL(DIV(EXTRACT(MINUTE FROM submission_timestamp), 60) * 60) MINUTE
    ) AS window_start,
    TIMESTAMP_ADD(
      TIMESTAMP_TRUNC(submission_timestamp, HOUR),
      INTERVAL((DIV(EXTRACT(MINUTE FROM submission_timestamp), 60) + 1) * 60) MINUTE
    ) AS window_end,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    'Firefox Focus for Android' AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
      -- Access experiment information.
      -- Additional iteration is necessary to aggregate total event count across experiments
      -- which is denoted with "*".
      -- Some clients are enrolled in multiple experiments, so simply summing up the totals
      -- across all the experiments would double count events.
    CASE
      experiment_index
      WHEN ARRAY_LENGTH(ping_info.experiments)
        THEN "*"
      ELSE ping_info.experiments[SAFE_OFFSET(experiment_index)].key
    END AS experiment,
    CASE
      experiment_index
      WHEN ARRAY_LENGTH(ping_info.experiments)
        THEN "*"
      ELSE ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch
    END AS experiment_branch,
    COUNT(*) AS total_events
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_live.events_v1`
  CROSS JOIN
    UNNEST(events) AS event,
      -- Iterator for accessing experiments.
      -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
  WHERE
    DATE(submission_timestamp) >= "2023-12-31"
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
