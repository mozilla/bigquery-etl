CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users_aggregates`
AS
SELECT
  *,
  CAST(`mozfun.norm.truncate_version`(os_version, "major") AS INTEGER) AS os_version_major,
  CAST(`mozfun.norm.truncate_version`(os_version, "minor") AS INTEGER) AS os_version_minor,
  COALESCE(
    `mozfun.norm.windows_version_info`(os, os_version, windows_build_number),
    os_version
  ) AS os_version_build,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_active_users_aggregates_v1`
