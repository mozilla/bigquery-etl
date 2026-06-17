WITH constants AS (
  SELECT
    1 << 0 AS PROCESS_TYPE_MAIN,
    1 << 1 AS PROCESS_TYPE_CONTENT,
    1 << 2 AS PROCESS_TYPE_GMPLUGIN,
    1 << 3 AS PROCESS_TYPE_GPU,
    1 << 4 AS PROCESS_TYPE_RDD,
    1 << 5 AS PROCESS_TYPE_SOCKET,
    1 << 6 AS PROCESS_TYPE_UTILITY,
    1 << 0 AS CLASSIFY_NONE,
    1 << 1 AS CLASSIFY_OOM,
    1 << 2 AS CLASSIFY_BACKGROUND_TASK,
    1 << 3 AS CLASSIFY_SHUTDOWN_HANG
),
crashes AS (
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    normalized_os AS os,
    crash_app_channel AS channel,
    CAST(mozfun.norm.extract_version(crash_app_display_version, "major") AS INT) AS major_version,
    (
      CASE
        metrics.string.crash_process_type
        WHEN 'main'
          THEN 1 << 0  -- PROCESS_TYPE_MAIN
        WHEN 'content'
          THEN 1 << 1  -- PROCESS_TYPE_CONTENT
        WHEN 'gmplugin'
          THEN 1 << 2  -- PROCESS_TYPE_GMPLUGIN
        WHEN 'gpu'
          THEN 1 << 3  -- PROCESS_TYPE_GPU
        WHEN 'rdd'
          THEN 1 << 4  -- PROCESS_TYPE_RDD
        WHEN 'socket'
          THEN 1 << 5  -- PROCESS_TYPE_SOCKET
        WHEN 'utility'
          THEN 1 << 6  -- PROCESS_TYPE_UTILITY
        ELSE 0
      END
    ) AS process_type_bit,
    (
      CASE
        WHEN metrics.quantity.memory_oom_allocation_size IS NOT NULL
          THEN 1 << 1  -- CLASSIFY_OOM
        WHEN metrics.string.crash_background_task_name IS NOT NULL
          THEN 1 << 2  -- CLASSIFY_BACKGROUND_TASK
        WHEN metrics.object.crash_async_shutdown_timeout IS NOT NULL
          OR metrics.object.crash_quota_manager_shutdown_timeout IS NOT NULL
          THEN 1 << 3  -- CLASSIFY_SHUTDOWN_HANG
        ELSE 1 << 0  -- CLASSIFY_NONE
      END
    ) AS classification_bit,
    (
      metrics.string.crash_minidump_sha256_hash = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
      OR metrics.string.crash_minidump_sha256_hash IS NULL
    )
    AND metrics.string.crash_process_type = 'main'
    AND NOT (
      SAFE_CAST(REGEXP_SUBSTR(crash_app_display_version, "[0-9]*") AS INT) > 149
      OR (
        SAFE_CAST(REGEXP_SUBSTR(crash_app_display_version, "[0-9]*") AS INT) = 149
        AND crash_app_build >= '20260128215503'
      )
    ) AS count_half
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_crashes`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND crash_app_channel IN ('release', 'beta', 'nightly', 'esr')
    AND metrics.string.crash_process_type IN (
      'main',
      'content',
      'gmplugin',
      'gpu',
      'rdd',
      'socket',
      'utility'
    )
),
client_daily_crashes AS (
  SELECT
    client_id,
    submission_date,
    os,
    channel,
    BIT_OR(process_type_bit) AS process_types_bitset,
    major_version,
    BIT_OR(classification_bit) AS classifications_bitset,
    count_half
    AND process_type_bit = (1 << 0) AS main_count_half
  FROM
    crashes
  GROUP BY
    ALL
)
SELECT
  submission_date,
  os,
  channel,
  major_version,
  process_types_bitset,
  classifications_bitset,
  COUNT(DISTINCT IF(main_count_half, client_id, NULL)) / 2 + COUNT(
    DISTINCT IF(main_count_half, NULL, client_id)
  ) AS crashing_users
FROM
  client_daily_crashes
GROUP BY
  ALL
