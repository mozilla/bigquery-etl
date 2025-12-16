WITH crashes AS (
  -- Crash records from Firefox desktop, excluding OS "Other" and "iOS"
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    normalized_os AS os,
    crash_app_channel AS channel,
    metrics.string.crash_process_type AS process_type,
    SAFE_CAST(REGEXP_SUBSTR(crash_app_display_version, "[0-9]*") AS INT) AS major_version,
    metrics.quantity.memory_oom_allocation_size AS memory_oom_allocation_size,
    metrics.string.crash_background_task_name AS crash_background_task_name,
    CASE
      WHEN metrics.object.crash_async_shutdown_timeout IS NOT NULL
        THEN TO_JSON_STRING(metrics.object.crash_async_shutdown_timeout)
      ELSE NULL
    END AS crash_async_shutdown_timeout,
    CASE
      WHEN metrics.object.crash_quota_manager_shutdown_timeout IS NOT NULL
        THEN TO_JSON_STRING(metrics.object.crash_quota_manager_shutdown_timeout)
      ELSE NULL
    END AS crash_quota_manager_shutdown_timeout,
    (
      metrics.string.crash_minidump_sha256_hash = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
      OR metrics.string.crash_minidump_sha256_hash IS NULL
    ) AS no_minidump
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_crashes`
  WHERE
    DATE(submission_timestamp) = @submission_date
)
  -- Aggregate distinct crashing users per OS, channel, major_version, process_type, and other crash details
SELECT
  submission_date,
  os,
  channel,
  major_version,
  process_type,
  COALESCE(memory_oom_allocation_size, NULL) AS memory_oom_allocation_size,
  COALESCE(crash_background_task_name, NULL) AS crash_background_task_name,
  COALESCE(crash_async_shutdown_timeout, NULL) AS crash_async_shutdown_timeout,
  COALESCE(crash_quota_manager_shutdown_timeout, NULL) AS crash_quota_manager_shutdown_timeout,
  COUNT(DISTINCT IF(no_minidump AND process_type = 'main', client_id, NULL)) / 2 + COUNT(
    DISTINCT IF(no_minidump AND process_type = 'main', NULL, client_id)
  ) AS crashing_users
FROM
  crashes
GROUP BY
  ALL
