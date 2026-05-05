CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.crashes_daily`
AS
SELECT
  * EXCEPT (process_crash_counts, process_shutdown_kill_crash_counts),
  -- Upper limits set at 99.9th percentile from https://sql.telemetry.mozilla.org/queries/94391/source
  LEAST(mozfun.map.get_key(process_crash_counts, "main"), 41) AS main_crash_count,
  LEAST(mozfun.map.get_key(process_crash_counts, "content"), 51) AS content_crash_count,
  LEAST(mozfun.map.get_key(process_crash_counts, "gpu"), 58) AS gpu_crash_count,
  LEAST(mozfun.map.get_key(process_crash_counts, "rdd"), 731) AS rdd_crash_count,
  LEAST(mozfun.map.get_key(process_crash_counts, "socket"), 99) AS socket_crash_count,
  LEAST(mozfun.map.get_key(process_crash_counts, "utility"), 1167) AS utility_crash_count,
  LEAST(mozfun.map.get_key(process_crash_counts, "vr"), 28) AS vr_crash_count,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.crashes_daily_v1`
