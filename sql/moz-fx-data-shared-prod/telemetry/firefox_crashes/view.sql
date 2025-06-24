CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_crashes`
AS
SELECT
  *,
  COALESCE(metrics.string.crash_app_build, client_info.app_build) AS crash_app_build,
  COALESCE(metrics.string.crash_app_channel, client_info.app_channel) AS crash_app_channel,
  COALESCE(
    metrics.string.crash_app_display_version,
    client_info.app_display_version
  ) AS crash_app_display_version,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_crashes_v1`
