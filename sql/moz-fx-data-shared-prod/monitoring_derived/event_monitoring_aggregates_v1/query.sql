-- Generated via ./bqetl generate glean_usage
WITH base_firefox_desktop_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Desktop" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_firefox_desktop_newtab_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Desktop" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_firefox_desktop_prototype_no_code_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Desktop" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.prototype_no_code_events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_firefox_desktop_urlbar_keyword_exposure_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Desktop" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_keyword_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_firefox_desktop_urlbar_potential_exposure_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Desktop" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_potential_exposure_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
firefox_desktop_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (
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
        base_firefox_desktop_prototype_no_code_events_v1
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_crashreporter_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Crash Reporter" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_crashreporter_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
firefox_crashreporter_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_firefox_crashreporter_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_background_update_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Desktop Background Update Task" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_update_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
firefox_desktop_background_update_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_firefox_desktop_background_update_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_background_defaultagent_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Desktop Default Agent Task" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
firefox_desktop_background_defaultagent_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_firefox_desktop_background_defaultagent_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_pine_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Pinebuild" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.pine_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
pine_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_pine_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_home_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_firefox_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_beta_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_beta_home_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_beta_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_firefox_beta_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_fenix_home_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_fenix_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_fenix_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fenix_nightly_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_fenix_nightly_home_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_fenix_nightly_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_fenix_nightly_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_fennec_aurora_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_fennec_aurora_home_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.home_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_fennec_aurora_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_fennec_aurora_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefox_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefox_first_session_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefox_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_firefox_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxbeta_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxbeta_first_session_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxbeta_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_firefoxbeta_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_fennec_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_fennec_first_session_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.first_session_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_fennec_metrics_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.metrics_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_fennec_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_reference_browser_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Reference Browser" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_reference_browser_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_reference_browser_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_tv_firefox_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Fire TV" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_tv_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_tv_firefox_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_tv_firefox_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_vrbrowser_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Reality" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_vrbrowser_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_vrbrowser_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_vrbrowser_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozilla_lockbox_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Lockwise for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozilla_lockbox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
mozilla_lockbox_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_mozilla_lockbox_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_lockbox_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Lockwise for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_lockbox_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_ios_lockbox_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_mozregression_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "mozregression" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_mozregression_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_mozregression_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_burnham_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Burnham" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.burnham_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
burnham_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_burnham_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozphab_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "mozphab" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozphab_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
mozphab_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_mozphab_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_connect_firefox_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox for Echo Show" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_connect_firefox_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_connect_firefox_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_connect_firefox_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefoxreality_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Reality for PC-connected VR platforms" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_firefoxreality_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_firefoxreality_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozilla_mach_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "mach" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozilla_mach_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
mozilla_mach_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_mozilla_mach_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_focus_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Focus for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_focus_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_ios_focus_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_klar_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Klar for iOS" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_klar_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_ios_klar_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_focus_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Focus for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_focus_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_focus_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_focus_beta_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Focus for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_focus_beta_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_focus_beta_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_focus_nightly_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Focus for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_focus_nightly_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_focus_nightly_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_klar_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Klar for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_klar_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_klar_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_org_mozilla_klar_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_bergamot_custom_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Bergamot Translator" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.custom_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_bergamot_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Bergamot Translator" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_bergamot_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_translations_custom_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Translations" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_translations_stable.custom_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_firefox_translations_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Translations" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_translations_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
firefox_translations_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_daemonsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_mozillavpn_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_mozillavpn_extensionsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_mozillavpn_main_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_mozillavpn_vpnsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
mozillavpn_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_firefox_vpn_daemonsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_vpn_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_vpn_extensionsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_vpn_main_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_firefox_vpn_vpnsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_firefox_vpn_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_daemonsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_extensionsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_main_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_vpnsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_firefoxvpn_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_org_mozilla_ios_firefoxvpn_network_extension_daemonsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.daemonsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_network_extension_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_network_extension_extensionsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.extensionsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_network_extension_main_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.main_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_org_mozilla_ios_firefoxvpn_network_extension_vpnsession_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.vpnsession_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
org_mozilla_ios_firefoxvpn_network_extension_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mozillavpn_backend_cirrus_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla VPN Cirrus Sidecar" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
mozillavpn_backend_cirrus_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_mozillavpn_backend_cirrus_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_glean_dictionary_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Glean Dictionary" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.glean_dictionary_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
glean_dictionary_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_glean_dictionary_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_mdn_yari_action_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Developer Network" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.action_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_mdn_yari_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Developer Network" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.mdn_yari_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
mdn_yari_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_mdn_yari_action_v1 UNION ALL SELECT * FROM base_mdn_yari_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_bedrock_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "www.mozilla.org" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_bedrock_interaction_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "www.mozilla.org" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.interaction_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_bedrock_non_interaction_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "www.mozilla.org" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.bedrock_stable.non_interaction_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
bedrock_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_viu_politica_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Viu Politica" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_viu_politica_main_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Viu Politica" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.main_events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_viu_politica_video_index_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Viu Politica" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.viu_politica_stable.video_index_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
viu_politica_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_treeherder_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Treeherder" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.treeherder_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
treeherder_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_treeherder_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_firefox_desktop_background_tasks_background_tasks_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Desktop background tasks" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.background_tasks_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
base_firefox_desktop_background_tasks_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Desktop background tasks" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
firefox_desktop_background_tasks_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_accounts_frontend_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Accounts Frontend" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.accounts_frontend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
accounts_frontend_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_accounts_frontend_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_accounts_backend_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Accounts Backend" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.accounts_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
accounts_backend_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_accounts_backend_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_accounts_cirrus_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Accounts (Cirrus)" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.accounts_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
accounts_cirrus_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_accounts_cirrus_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_monitor_cirrus_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Monitor (Cirrus)" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.monitor_cirrus_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
monitor_cirrus_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_monitor_cirrus_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_debug_ping_view_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Glean Debug Ping Viewer" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.debug_ping_view_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
debug_ping_view_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_debug_ping_view_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_monitor_frontend_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Monitor (Frontend)" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.monitor_frontend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
monitor_frontend_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_monitor_frontend_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_monitor_backend_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Mozilla Monitor (Backend)" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.monitor_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
monitor_backend_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_monitor_backend_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_relay_backend_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Firefox Relay Backend" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.relay_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
relay_backend_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_relay_backend_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_gleanjs_docs_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Glean.js Documentation" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.gleanjs_docs_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
gleanjs_docs_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_gleanjs_docs_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_thunderbird_desktop_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Thunderbird" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.thunderbird_desktop_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
thunderbird_desktop_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_thunderbird_desktop_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_net_thunderbird_android_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Thunderbird for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
net_thunderbird_android_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_net_thunderbird_android_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_net_thunderbird_android_beta_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Thunderbird for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
net_thunderbird_android_beta_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_net_thunderbird_android_beta_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_net_thunderbird_android_daily_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Thunderbird for Android" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.net_thunderbird_android_daily_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
net_thunderbird_android_daily_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_net_thunderbird_android_daily_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_syncstorage_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Sync Storage" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.syncstorage_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
syncstorage_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_syncstorage_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_glam_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "GLAM" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.glam_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
glam_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_glam_events_v1)
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
    normalized_app_name,
    channel,
    version,
    experiment,
    experiment_branch
),
base_subscription_platform_backend_events_v1 AS (
  SELECT
    submission_timestamp,
    event.category AS event_category,
    event.name AS event_name,
    event_extra.key AS event_extra_key,
    normalized_country_code AS country,
    "Subscription Platform" AS normalized_app_name,
    client_info.app_channel AS channel,
    client_info.app_display_version AS version,
        -- experiments[ARRAY_LENGTH(experiments)] will be set to '*'
    COALESCE(ping_info.experiments[SAFE_OFFSET(experiment_index)].key, '*') AS experiment,
    COALESCE(
      ping_info.experiments[SAFE_OFFSET(experiment_index)].value.branch,
      '*'
    ) AS experiment_branch,
  FROM
    `moz-fx-data-shared-prod.subscription_platform_backend_stable.events_v1`
  CROSS JOIN
    UNNEST(events) AS event
  CROSS JOIN
        -- Iterator for accessing experiments.
        -- Add one more for aggregating events across all experiments
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
  LEFT JOIN
    UNNEST(event.extra) AS event_extra
),
subscription_platform_backend_aggregated AS (
  SELECT
    @submission_date AS submission_date,
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
    (SELECT * FROM base_subscription_platform_backend_events_v1)
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
  firefox_desktop_background_update_aggregated
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
