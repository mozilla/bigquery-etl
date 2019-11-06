WITH crash_ping_agg AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    environment.build.build_id,
    metadata.uri.app_name,
    IF(metadata.uri.app_name = 'Fennec'
       AND environment.system.os.name = 'Linux',
       'Android',
       environment.system.os.name) AS os,
    environment.system.os.version AS os_version,
    environment.build.architecture,
    normalized_country_code AS country,
    IF(
      payload.process_type = 'main'
      OR payload.process_type IS NULL,
      1,
      0
    ) AS main_crash,
    IF(
      REGEXP_CONTAINS(payload.process_type, 'content'),
      1,
      0
    ) AS content_crash,
    IF(payload.metadata.startup_crash = '1', 1, 0) AS startup_crash,
    IF(
      REGEXP_CONTAINS(payload.metadata.ipc_channel_error, 'ShutDownKill'),
      1,
      0
    ) AS content_shutdown_crash,
    -- 0 columns to match main pings
    0 AS usage_hours,
    0 AS gpu_crashes,
    0 AS plugin_crashes,
    0 AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry_live.crash_v4`
),
main_ping_agg AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    environment.build.version,
    application.display_version,
    environment.build.build_id,
    metadata.uri.app_name,
    environment.system.os.name AS os,
    environment.system.os.version AS os_version,
    environment.build.architecture,
    normalized_country_code AS country,
    -- 0 columns to match crash ping
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    LEAST(GREATEST(payload.info.subsession_length / 3600, 0), 25) AS usage_hours,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gpu'), 0) AS gpu_crashes,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin'), 0) AS plugin_crashes,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gmplugin'), 0) AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry_live.main_v4`
),
core_ping_agg AS (
  SELECT
    submission_timestamp,
    normalized_channel AS channel,
    metadata.uri.app_version AS version,
    COALESCE(display_version, metadata.uri.app_version) AS display_version,
    metadata.uri.app_build_id AS build_id,
    metadata.uri.app_name,
    os,
    osversion AS os_version,
    arch AS architecture,
    normalized_country_code AS country,
    -- 0 columns to match crash ping
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    LEAST(GREATEST(durations / 3600, 0), 25) AS usage_hours,
    0 AS gpu_crashes,
    0 AS plugin_crashes,
    0 AS gmplugin_crashes
  FROM
    `moz-fx-data-shared-prod.telemetry_live.core_v10`
),
combined_crashes AS (
  SELECT
    *
  FROM
    crash_ping_agg
  UNION ALL
  SELECT
    *
  FROM
    main_ping_agg
  UNION ALL
  SELECT
    *
  FROM
    core_ping_agg
)

SELECT
  DATE(submission_timestamp) AS submission_date,
  udf_round_timestamp_to_minute(submission_timestamp, 5) AS window_start,
  channel,
  version,
  display_version,
  build_id,
  app_name as application,
  os as os_name,
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
  SUM(usage_hours) AS usage_hours
FROM
  combined_crashes
WHERE
  DATE(submission_timestamp) = '2019-11-05' -- TODO: USE param
  AND DATE_DIFF(  -- Only use builds from the last 6 months
    CURRENT_DATE(),
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
