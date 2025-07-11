-- Generated via `usage_reporting` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.usage_reporting_active_users_aggregates`
AS
SELECT
  * REPLACE (
    CASE
      WHEN app_name = "firefox_desktop"
        THEN "Firefox Desktop"
      WHEN LOWER(app_name) = "firefox_desktop mozillaonline"
        THEN "Firefox Desktop MozillaOnline"
      WHEN app_name = "fenix"
        THEN "Fenix"
      WHEN LOWER(app_name) = "fenix mozillaonline"
        THEN "Fenix MozillaOnline"
      WHEN app_name = "focus_android"
        THEN "Focus Android"
      WHEN app_name = "klar_android"
        THEN "Klar Android"
      WHEN app_name = "firefox_ios"
        THEN "Firefox iOS"
      WHEN app_name = "focus_ios"
        THEN "Focus iOS"
      WHEN app_name = "klar_ios"
        THEN "Klar iOS"
      ELSE app_name
    END AS app_name
  ),
  `mozfun.norm.browser_version_info`(app_version).major_version AS app_version_major,
  `mozfun.norm.browser_version_info`(app_version).minor_version AS app_version_minor,
  `mozfun.norm.browser_version_info`(app_version).patch_revision AS app_version_patch_revision,
  `mozfun.norm.browser_version_info`(app_version).is_major_release AS app_version_is_major_release,
  CAST(`mozfun.norm.extract_version`(os_version, "major") AS INTEGER) AS os_version_major,
  CAST(`mozfun.norm.extract_version`(os_version, "minor") AS INTEGER) AS os_version_minor,
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.usage_reporting_active_users_aggregates_v1`
