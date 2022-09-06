CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.crashes_daily`
AS
SELECT
  * EXCEPT (process_crash_counts, process_shutdown_kill_crash_counts),
  mozfun.map.get_key(process_crash_counts, "main") AS main_crash_count,
  mozfun.map.get_key(process_crash_counts, "content") AS content_crash_count,
  mozfun.map.get_key(process_crash_counts, "gpu") AS gpu_crash_count,
  mozfun.map.get_key(process_crash_counts, "rdd") AS rdd_crash_count,
  mozfun.map.get_key(process_crash_counts, "socket") AS socket_crash_count,
  mozfun.map.get_key(process_crash_counts, "utility") AS utility_crash_count,
  mozfun.map.get_key(process_crash_counts, "vr") AS vr_crash_count,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.crashes_daily_v1`
