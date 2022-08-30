CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.telemetry_derived.crashes_daily_v1`
PARTITION BY
  (submission_date)
AS
WITH crashes AS (
  SELECT
    *,
    COALESCE(payload.process_type, "main") AS process_type,
    REGEXP_CONTAINS(COALESCE(payload.metadata.ipc_channel_error, ""), "ShutDownKill") AS is_shutdown_kill,
  FROM
    mozdata.telemetry.crash
  WHERE
    DATE(submission_timestamp) >= "2022-08-01"
)
SELECT
  client_id,
  sample_id,
  DATE(submission_timestamp) AS submission_date,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_app_name)) AS application_name,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os)) AS os_name,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_os_version)) AS os_version,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_channel)) AS channel,
  mozfun.stats.mode_last(ARRAY_AGG(normalized_country_code)) AS country_code,
  mozfun.map.sum(ARRAY_AGG(IF(NOT is_shutdown_kill, STRUCT(process_type AS key, 1 AS value), NULL) IGNORE NULLS)) AS process_crash_counts,
  mozfun.map.sum(ARRAY_AGG(IF(is_shutdown_kill, STRUCT(process_type AS key, 1 AS value), NULL) IGNORE NULLS)) AS process_shutdown_kill_crash_counts,
FROM
  crashes
GROUP BY
  client_id,
  sample_id,
  submission_date,
