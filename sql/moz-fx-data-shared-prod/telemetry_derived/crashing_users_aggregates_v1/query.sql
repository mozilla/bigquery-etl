DECLARE PROCESS_TYPE_MAIN INT64 DEFAULT 1 << 0;

DECLARE PROCESS_TYPE_CONTENT INT64 DEFAULT 1 << 1;

DECLARE PROCESS_TYPE_GMPLUGIN INT64 DEFAULT 1 << 2;

DECLARE PROCESS_TYPE_GPU INT64 DEFAULT 1 << 3;

DECLARE PROCESS_TYPE_RDD INT64 DEFAULT 1 << 4;

DECLARE PROCESS_TYPE_SOCKET INT64 DEFAULT 1 << 5;

DECLARE PROCESS_TYPE_UTILITY INT64 DEFAULT 1 << 6;

DECLARE CLASSIFY_NONE INT64 DEFAULT 1 << 0;

DECLARE CLASSIFY_OOM INT64 DEFAULT 1 << 1;

DECLARE CLASSIFY_BACKGROUND_TASK INT64 DEFAULT 1 << 2;

DECLARE CLASSIFY_SHUTDOWN_HANG INT64 DEFAULT 1 << 3;

WITH crashes AS (
  SELECT
    client_info.client_id AS client_id,
    DATE(submission_timestamp) AS submission_date,
    normalized_os AS os,
    crash_app_channel AS channel,
    SAFE_CAST(REGEXP_SUBSTR(crash_app_display_version, "[0-9]*") AS INT) AS major_version,
    (
      CASE
        metrics.string.crash_process_type
        WHEN 'main'
          THEN PROCESS_TYPE_MAIN
        WHEN 'content'
          THEN PROCESS_TYPE_CONTENT
        WHEN 'gmplugin'
          THEN PROCESS_TYPE_GMPLUGIN
        WHEN 'gpu'
          THEN PROCESS_TYPE_GPU
        WHEN 'rdd'
          THEN PROCESS_TYPE_RDD
        WHEN 'socket'
          THEN PROCESS_TYPE_SOCKET
        WHEN 'utility'
          THEN PROCESS_TYPE_UTILITY
        ELSE 0
      END
    ) AS process_type_bit,
    --SAFE_CAST(REGEXP_SUBSTR(crash_app_display_version,"[0-9]*") as INT) as major_version,
    (
      CASE
        WHEN metrics.quantity.memory_oom_allocation_size IS NOT NULL
          THEN CLASSIFY_OOM
        WHEN metrics.string.crash_background_task_name IS NOT NULL
          THEN CLASSIFY_BACKGROUND_TASK
        WHEN metrics.object.crash_async_shutdown_timeout IS NOT NULL
          OR metrics.object.crash_quota_manager_shutdown_timeout IS NOT NULL
          THEN CLASSIFY_SHUTDOWN_HANG
        ELSE CLASSIFY_NONE
      END
    ) AS classification_bit,
    (
      metrics.string.crash_minidump_sha256_hash = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
      OR metrics.string.crash_minidump_sha256_hash IS NULL
    ) AS no_minidump
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
    -- Main crashes without a minidump will be double counted because they are submitted from the crash reporter and firefox, so we distinguish those clients
    no_minidump
    AND process_type_bit = PROCESS_TYPE_MAIN AS main_no_minidump
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
    -- Count main crashes without a minidump separately to halve them.
  COUNT(DISTINCT IF(main_no_minidump, client_id, NULL)) / 2 + COUNT(
    DISTINCT IF(main_no_minidump, NULL, client_id)
  ) AS crashing_users
FROM
  client_daily_crashes
GROUP BY
  ALL
