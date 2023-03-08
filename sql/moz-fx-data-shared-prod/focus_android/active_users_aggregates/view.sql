CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.active_users_aggregates`
AS
SELECT
  * REPLACE (COALESCE(country, '??') AS country),
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info
FROM
  `moz-fx-data-shared-prod.focus_android_derived.active_users_aggregates_v1`
WHERE
  app_name != 'Focus Android Glean'
  AND app_name != 'Focus Android Glean BrowserStack'
