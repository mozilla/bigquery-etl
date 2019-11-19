CREATE TEMP FUNCTION
  udf_json_extract_int_map (input STRING) AS (ARRAY(
    SELECT
      STRUCT(CAST(SPLIT(entry, ':')[OFFSET(0)] AS INT64) AS key,
             CAST(SPLIT(entry, ':')[OFFSET(1)] AS INT64) AS value)
    FROM
      UNNEST(SPLIT(REPLACE(TRIM(input, '{}'), '"', ''), ',')) AS entry
    WHERE
      LENGTH(entry) > 0 ));
CREATE TEMP FUNCTION
  udf_json_extract_histogram (input STRING) AS (STRUCT(
    CAST(JSON_EXTRACT_SCALAR(input, '$.bucket_count') AS INT64) AS bucket_count,
    CAST(JSON_EXTRACT_SCALAR(input, '$.histogram_type') AS INT64) AS histogram_type,
    CAST(JSON_EXTRACT_SCALAR(input, '$.sum') AS INT64) AS `sum`,
    ARRAY(
      SELECT
        CAST(bound AS INT64)
      FROM
        UNNEST(SPLIT(TRIM(JSON_EXTRACT(input, '$.range'), '[]'), ',')) AS bound) AS `range`,
    udf_json_extract_int_map(JSON_EXTRACT(input, '$.values')) AS `values` ));
CREATE TEMP FUNCTION udf_histogram_get_sum(histogram_list ANY TYPE, target_key STRING) AS (
  (
    SELECT
      udf_json_extract_histogram(value).sum
    FROM
      UNNEST(histogram_list)
    WHERE
      key = target_key
    LIMIT
      1
  )
);
CREATE TEMP FUNCTION udf_round_timestamp_to_minute(timestamp_expression TIMESTAMP, minute INT64) AS (
  TIMESTAMP_SECONDS(
    CAST((FLOOR(UNIX_SECONDS(timestamp_expression) / (minute * 60)) * minute * 60) AS INT64)
  )
);
--
-- Get pings from the last day from live tables, stable tables for older
WITH crash_pings AS (
  SELECT
    *
  FROM
    telemetry_live.crash_v4
  WHERE
    DATE_DIFF(CURRENT_DATE, DATE(submission_timestamp), DAY) <= 1
  UNION ALL
  SELECT
    *
  FROM
    telemetry_stable.crash_v4
  WHERE
    DATE_DIFF(CURRENT_DATE, DATE(submission_timestamp), DAY) > 1
),
main_pings AS (
  SELECT
    *
  FROM
    telemetry_live.main_v4
  WHERE
    DATE_DIFF(CURRENT_DATE, DATE(submission_timestamp), DAY) <= 1
  UNION ALL
  SELECT
    *
  FROM
    telemetry_stable.main_v4
  WHERE
    DATE_DIFF(CURRENT_DATE, DATE(submission_timestamp), DAY) > 1
),
core_pings AS (
  SELECT
    *
  FROM
    telemetry.core_live
  WHERE
    DATE_DIFF(CURRENT_DATE, DATE(submission_timestamp), DAY) <= 1
  UNION ALL
  SELECT
    *
  FROM
    telemetry.core
  WHERE
    DATE_DIFF(CURRENT_DATE, DATE(submission_timestamp), DAY) > 1
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
    -- 0 for values retrieved from main/core pings
    0 AS usage_hours,
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
    environment.system.os.name AS os,
    environment.system.os.version AS os_version,
    environment.build.architecture,
    normalized_country_code AS country,
    -- 0 for values retrieved from crash pings
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    LEAST(GREATEST(payload.info.subsession_length / 3600, 0), 25) AS usage_hours,  -- protect against extreme values
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gpu'), 0) AS gpu_crashes,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'plugin'), 0) AS plugin_crashes,
    COALESCE(udf_histogram_get_sum(payload.keyed_histograms.subprocess_crashes_with_dump, 'gmplugin'), 0) AS gmplugin_crashes
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
    os,
    osversion AS os_version,
    arch AS architecture,
    normalized_country_code AS country,
    -- 0 for values retrieved from crash pings
    0 AS main_crash,
    0 AS content_crash,
    0 AS startup_crash,
    0 AS content_shutdown_crash,
    LEAST(GREATEST(durations / 3600, 0), 25) AS usage_hours,  -- protect against extreme values
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
  combined_ping_data
WHERE
  DATE(submission_timestamp) = @submission_date
  AND DATE_DIFF(  -- Only use builds from the last 6 months
    CURRENT_DATE,
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
