-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.usage_reporting_active_users_aggregates`
AS
SELECT
  *,
  `mozfun.norm.browser_version_info`(app_version).major_version AS app_version_major,
  `mozfun.norm.browser_version_info`(app_version).minor_version AS app_version_minor,
  `mozfun.norm.browser_version_info`(app_version).patch_revision AS app_version_patch_revision,
  `mozfun.norm.browser_version_info`(app_version).is_major_release AS app_version_is_major_release,
  CAST(`mozfun.norm.truncate_version`(os_version, "major") AS INTEGER) AS os_version_major,
  CAST(`mozfun.norm.truncate_version`(os_version, "minor") AS INTEGER) AS os_version_minor,
  COALESCE(
    `mozfun.norm.windows_version_info`(os, os_version, windows_build_number),
    os_version
  ) AS os_version_build,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.usage_reporting_active_users_aggregates_v1`
