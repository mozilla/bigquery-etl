WITH first_sessions AS (
    -- these are the sessions where clients were enrolled in the experiment
    -- since fission is enabled after restart, we want to filter them out from the main pings
  SELECT
    session_id
  FROM
    `moz-fx-data-shared-prod.telemetry.events`
  WHERE
    event_category = 'normandy'
    AND normalized_channel = 'nightly'
    AND event_method = 'enroll'
    AND submission_date = @submission_date
    AND event_string_value = 'bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100'
),
crash_ping_data_unfiltered AS (
  SELECT
    submission_timestamp,
    client_id,
    payload.session_id,
    CASE
    WHEN
      mozfun.map.get_key(
        environment.experiments,
        "bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100"
      ).branch = 'fission-enabled'
    THEN
      'enabled'
    WHEN
      mozfun.map.get_key(
        environment.experiments,
        "bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100"
      ).branch = 'fission-disabled'
    THEN
      'disabled'
    END
    AS experiment_branch,
    environment.build.build_id,
    environment.system.os.name AS os_name,
    environment.system.os.version AS os_version,
    IF(payload.process_type = 'main' OR payload.process_type IS NULL, 1, 0) AS main_crash,
    IF(
      REGEXP_CONTAINS(payload.process_type, 'content')
      AND NOT REGEXP_CONTAINS(COALESCE(payload.metadata.ipc_channel_error, ''), 'ShutDownKill'),
      1,
      0
    ) AS content_crash,
    IF(payload.metadata.startup_crash = '1', 1, 0) AS startup_crash,
    IF(
      REGEXP_CONTAINS(payload.process_type, 'content')
      AND REGEXP_CONTAINS(payload.metadata.ipc_channel_error, 'ShutDownKill'),
      1,
      0
    ) AS content_shutdown_crash,
    IF(payload.metadata.oom_allocation_size IS NOT NULL, 1, 0) AS oom_crashes,
    IF(payload.metadata.ipc_channel_error = 'ShutDownKill', 1, 0) AS shutdown_kill_crashes,
    IF(payload.metadata.moz_crash_reason LIKE 'MOZ_CRASH%', 1, 0) AS shutdown_hangs,
    -- 0 for values retrieved from main ping
    0 AS usage_seconds,
    0 AS gpu_crashes,
    0 AS plugin_crashes,
    0 AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry.crash`
  WHERE
    normalized_channel = 'nightly'
    AND mozfun.map.get_key(
      environment.experiments,
      "bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100"
    ) IS NOT NULL
),
crash_ping_data AS (
  SELECT
    c.* EXCEPT (session_id)
  FROM
    crash_ping_data_unfiltered c
  LEFT JOIN
    first_sessions e
  ON
    c.session_id = e.session_id
  WHERE
    e.session_id IS NULL
),
main_ping_data_unfiltered AS (
  SELECT
    submission_timestamp,
    client_id,
    payload.info.session_id,
    CASE
    WHEN
      mozfun.map.get_key(
        environment.experiments,
        "bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100"
      ).branch = 'fission-enabled'
    THEN
      'enabled'
    WHEN
      mozfun.map.get_key(
        environment.experiments,
        "bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100"
      ).branch = 'fission-disabled'
    THEN
      'disabled'
    END
    AS experiment_branch,
    environment.build.build_id,
    environment.system.os.name AS os_name,
    environment.system.os.version AS os_version,
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    0 AS oom_crashes,
    0 AS shutdown_kill_crashes,
    0 AS shutdown_hangs,
    payload.info.subsession_length AS usage_seconds,
    COALESCE(
      `moz-fx-data-shared-prod`.udf.keyed_histogram_get_sum(
        payload.keyed_histograms.subprocess_crashes_with_dump,
        'gpu'
      ),
      0
    ) AS gpu_crashes,
    COALESCE(
      `moz-fx-data-shared-prod`.udf.keyed_histogram_get_sum(
        payload.keyed_histograms.subprocess_crashes_with_dump,
        'plugin'
      ),
      0
    ) AS plugin_crashes,
    COALESCE(
      `moz-fx-data-shared-prod`.udf.keyed_histogram_get_sum(
        payload.keyed_histograms.subprocess_crashes_with_dump,
        'gmplugin'
      ),
      0
    ) AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  WHERE
    normalized_channel = 'nightly'
    AND mozfun.map.get_key(
      environment.experiments,
      "bug-1660366-pref-ongoing-fission-nightly-experiment-nightly-83-100"
    ) IS NOT NULL
),
main_ping_data AS (
  SELECT
    m.* EXCEPT (session_id)
  FROM
    main_ping_data_unfiltered m
  LEFT JOIN
    first_sessions e
  ON
    m.session_id = e.session_id
  WHERE
    e.session_id IS NULL
),
combined_ping_data AS (
  SELECT
    *
  FROM
    crash_ping_data
  UNION ALL
  SELECT
    *
  FROM
    main_ping_data
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_id,
  experiment_branch,
  build_id,
  os_name,
  os_version,
  COUNT(*) AS count,
  SUM(main_crash) AS main_crashes,
  SUM(content_crash) AS content_crashes,
  SUM(startup_crash) AS startup_crashes,
  SUM(content_shutdown_crash) AS content_shutdown_crashes,
  SUM(oom_crashes) AS oom_crashes,
  SUM(shutdown_hangs) AS shutdown_hangs,
  SUM(gpu_crashes) AS gpu_crashes,
  SUM(plugin_crashes) AS plugin_crashes,
  SUM(gmplugin_crashes) AS gmplugin_crashes,
  SUM(
    LEAST(GREATEST(usage_seconds / 3600, 0), 24)
  ) AS usage_hours  -- protect against extreme values
FROM
  combined_ping_data
WHERE
  DATE(submission_timestamp) = @submission_date
GROUP BY
  submission_date,
  client_id,
  experiment_branch,
  build_id,
  os_name,
  os_version
