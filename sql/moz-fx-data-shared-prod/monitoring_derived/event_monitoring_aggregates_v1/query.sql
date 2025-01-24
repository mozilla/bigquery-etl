-- Generated via ./bqetl generate glean_usage
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Desktop" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.newtab_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.prototype_no_code_events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_keyword_exposure_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.urlbar_potential_exposure_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Crash Reporter" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_crashreporter_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Desktop Background Update Task" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_background_update_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Desktop Default Agent Task" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Pinebuild" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.pine_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_stable.home_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.home_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_stable.home_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.home_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.home_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for iOS" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.first_session_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for iOS" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.first_session_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for iOS" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.first_session_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.metrics_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Reference Browser" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Fire TV" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_tv_firefox_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Reality" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_vrbrowser_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Lockwise for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozilla_lockbox_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Lockwise for iOS" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "mozregression" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Burnham" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.burnham_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "mozphab" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozphab_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox for Echo Show" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_connect_firefox_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Reality for PC-connected VR platforms" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "mach" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozilla_mach_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Focus for iOS" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Klar for iOS" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Focus for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_focus_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Focus for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Focus for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Klar for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_klar_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Bergamot Translator" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.custom_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Translations" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_translations_stable.custom_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_translations_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla VPN" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozillavpn_stable.daemonsession_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozillavpn_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozillavpn_stable.main_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozillavpn_stable.vpnsession_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla VPN" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.daemonsession_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.main_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.vpnsession_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla VPN" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.daemonsession_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.main_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.vpnsession_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla VPN" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.daemonsession_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.main_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.vpnsession_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla VPN Cirrus Sidecar" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Glean Dictionary" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.glean_dictionary_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla Developer Network" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mdn_yari_stable.action_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.mdn_yari_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "www.mozilla.org" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.bedrock_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.bedrock_stable.interaction_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.bedrock_stable.non_interaction_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Viu Politica" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.viu_politica_stable.events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.viu_politica_stable.main_events_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.viu_politica_stable.video_index_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Treeherder" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.treeherder_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Desktop background tasks" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.background_tasks_v1`
    UNION ALL
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla Accounts Frontend" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.accounts_frontend_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla Accounts Backend" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.accounts_backend_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla Accounts (Cirrus)" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.accounts_cirrus_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla Monitor (Cirrus)" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.monitor_cirrus_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Glean Debug Ping Viewer" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.debug_ping_view_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla Monitor (Frontend)" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.monitor_frontend_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Mozilla Monitor (Backend)" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.monitor_backend_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Firefox Relay Backend" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.relay_backend_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Glean.js Documentation" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.gleanjs_docs_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Thunderbird" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.thunderbird_desktop_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Thunderbird for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.net_thunderbird_android_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Thunderbird for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.net_thunderbird_android_beta_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Thunderbird for Android" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.net_thunderbird_android_daily_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
UNION ALL
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
  event.category AS event_category,
  event.name AS event_name,
  event_extra.key AS event_extra_key,
  normalized_country_code AS country,
  "Sync Storage" AS normalized_app_name,
  channel,
  version,
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
  (
    SELECT
      submission_timestamp,
      events,
      normalized_country_code,
      client_info.app_channel AS channel,
      client_info.app_display_version AS version,
      STRUCT(
        ping_info.end_time,
        ARRAY(
          SELECT AS STRUCT
            key,
            STRUCT(
              value.branch,
              STRUCT(value.extra.type, value.extra.enrollment_id) AS extra
            ) AS value
          FROM
            UNNEST(ping_info.experiments)
            WITH OFFSET
          ORDER BY
            offset
        ) AS experiments,
        ping_info.ping_type,
        ping_info.seq,
        ping_info.start_time,
        ping_info.reason
      ) AS ping_info,
    FROM
      `moz-fx-data-shared-prod.syncstorage_stable.events_v1`
  )
CROSS JOIN
  UNNEST(events) AS event,
    -- Iterator for accessing experiments.
    -- Add one more for aggregating events across all experiments
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(ping_info.experiments))) AS experiment_index
LEFT JOIN
  UNNEST(event.extra) AS event_extra
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
