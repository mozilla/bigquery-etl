CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users_aggregates_full`
AS
SELECT
  *,
  `mozfun.norm.glean_windows_version_info`(
    os_grouped,
    os_version,
    windows_build_number
  ) AS windows_version
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_active_users_aggregates_v2`
