CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.widgets_client_daily`
AS
SELECT
  'Firefox Desktop' AS app_name,
  mozfun.norm.glean_windows_version_info(os, os_version, windows_build_number) AS windows_version,
  *,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.widgets_client_daily_v1`
