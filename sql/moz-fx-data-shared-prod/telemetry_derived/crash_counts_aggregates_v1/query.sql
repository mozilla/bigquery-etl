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
    SAFE_CAST(REGEXP_SUBSTR(crash_app_display_version, "[0-9]*") AS INT) AS major_version,
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
    ) AS main_no_minidump
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
-- Crash counts aggregation uses the following CTEs
-- To be able to eliminate outlier clients that crash a lot, we build a few queries to calculate per-client statistics.
per_client_crash_counts AS (
  SELECT
    client_id,
    submission_date,
    os,
    channel,
    process_type_bit,
    major_version,
    classification_bit,
    main_no_minidump,
    COUNT(*) AS crash_count
  FROM
    crashes
  GROUP BY
    ALL
),
/*
We want to exclude the crashiest clients per (submission_date, os, channel, process_type) combination, so group accordingly.

It's difficult to account for main_no_minidump here, but if we assume the main_no_minidump clients are evenly
distributed, the double-submissions shouldn't skew the statistics too much. Note that they may not be (given the
cause of no minidump is often an OOM condition), but the only other option is randomly sampling to reduce the
aggregation, which we want to avoid.
*/
crashes_per_client_quantiles AS (
  SELECT
    submission_date,
    os,
    channel,
    process_type_bit,
    APPROX_QUANTILES(crash_count, 100) AS quantiles,
  FROM
    per_client_crash_counts
  GROUP BY
    ALL
),
crashes_per_client AS (
  SELECT
    submission_date,
    os,
    channel,
    process_type_bit,
    major_version,
    classification_bit,
    main_no_minidump,
    COUNT(*) AS client_count,
    AVG(IF(crash_count <= quantiles[OFFSET(95)], crash_count, NULL)) AS average_95percentile,
    AVG(
      IF(
        crash_count <= quantiles[OFFSET(75)] + (
          quantiles[OFFSET(75)] - quantiles[OFFSET(25)]
        ) * 1.5,
        crash_count,
        NULL
      )
    ) AS average_no_outliers,
    AVG(crash_count) AS average,
  FROM
    per_client_crash_counts
  LEFT JOIN
    crashes_per_client_quantiles
    USING (submission_date, os, channel, process_type_bit)
  GROUP BY
    ALL
)
SELECT
  submission_date,
  os,
  channel,
  major_version,
  process_type_bit,
  classification_bit,
  SUM(client_count * average / IF(main_no_minidump, 2, 1)) AS total_crashes,
  SUM(client_count * average_95percentile / IF(main_no_minidump, 2, 1)) AS total_95percentile,
  SUM(client_count * average_no_outliers / IF(main_no_minidump, 2, 1)) AS total_no_outliers
FROM
  crashes_per_client
GROUP BY
  ALL
