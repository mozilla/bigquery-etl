-- Get pings from the last day from live tables, stable tables for older
WITH crash_pings AS (
  SELECT
    *
  FROM
    telemetry_live.crash_v4
  WHERE
    DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  UNION ALL
  SELECT
    *
  FROM
    telemetry_stable.crash_v4
  WHERE
    DATE(submission_timestamp) < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
),
main_pings AS (
  SELECT
    *
  FROM
    telemetry_live.main_v5
  WHERE
    DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  UNION ALL
  SELECT
    *
  FROM
    telemetry_stable.main_v5
  WHERE
    DATE(submission_timestamp) < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
),
core_pings AS (
  SELECT
    *
  FROM
    telemetry_derived.core_live
  WHERE
    DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
  UNION ALL
  SELECT
    *
  FROM
    telemetry.core
  WHERE
    DATE(submission_timestamp) < DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
),
-- Get main, content, startup, and content_shutdown crashes from crash pings
crash_ping_data AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    environment.build.build_id,
    metadata.uri.app_name,
    IF(
      metadata.uri.app_name = 'Fennec'
      AND normalized_os = 'Linux',
      'Android',
      normalized_os
    ) AS os,
    normalized_os_version AS os_version,
    environment.build.architecture,
    normalized_country_code AS country,
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
    -- 0 for values retrieved from main/core pings
    0 AS usage_seconds,
    0 AS gpu_crashes,
    0 AS plugin_crashes,
    0 AS gmplugin_crashes
  FROM
    crash_pings
),
-- Get desktop usage hours and gpu, plugin, and gmplugin crashes from main pings
main_ping_data AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    environment.build.build_id,
    metadata.uri.app_name,
    normalized_os AS os,
    normalized_os_version AS os_version,
    environment.build.architecture,
    normalized_country_code AS country,
    -- 0 for values retrieved from crash pings
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    payload.info.subsession_length AS usage_seconds,
    COALESCE(
      udf.keyed_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gpu'),
      0
    ) AS gpu_crashes,
    COALESCE(
      udf.keyed_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin'),
      0
    ) AS plugin_crashes,
    COALESCE(
      udf.keyed_histogram_get_sum(
        payload.keyed_histograms.subprocess_crashes_with_dump,
        'gmplugin'
      ),
      0
    ) AS gmplugin_crashes
  FROM
    main_pings
),
-- Get mobile usage hours from core pings
core_ping_data AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    metadata.uri.app_version AS version,
    COALESCE(display_version, metadata.uri.app_version) AS display_version,
    metadata.uri.app_build_id AS build_id,
    metadata.uri.app_name,
    normalized_os,
    normalized_os_version AS os_version,
    arch AS architecture,
    normalized_country_code AS country,
    -- 0 for values retrieved from crash pings
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    durations AS usage_seconds,
    0 AS gpu_crashes,
    0 AS plugin_crashes,
    0 AS gmplugin_crashes
  FROM
    core_pings
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
  UNION ALL
  SELECT
    *
  FROM
    core_ping_data
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  udf.round_timestamp_to_minute(submission_timestamp, 5) AS window_start,
  channel,
  version,
  display_version,
  build_id,
  app_name AS application,
  os AS os_name,
  os_version,
  architecture,
  country,
  COUNT(*) AS count,
  SUM(main_crash) AS main_crashes,
  SUM(content_crash) AS content_crashes,
  SUM(startup_crash) AS startup_crashes,
  SUM(content_shutdown_crash) AS content_shutdown_crashes,
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
  AND DATE_DIFF(  -- Only use builds from the last 6 months
    @submission_date,
    SAFE.PARSE_DATE('%Y%m%d', SUBSTR(build_id, 0, 8)),
    MONTH
  ) <= 6
GROUP BY
  submission_date,
  window_start,
  channel,
  version,
  display_version,
  build_id,
  application,
  os_name,
  os_version,
  architecture,
  country
