CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.unified_metrics`
AS
SELECT
  * REPLACE (COALESCE(country, '??') AS country),
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info
FROM
  `moz-fx-data-shared-prod.telemetry_derived.unified_metrics_v1`
WHERE
  normalized_app_name != 'Focus Android Glean'
  AND normalized_app_name != 'Focus Android Glean BrowserStack'
