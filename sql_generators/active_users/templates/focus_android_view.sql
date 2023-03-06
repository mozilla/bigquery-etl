CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.active_users_aggregates`
AS
SELECT
  * REPLACE (COALESCE(country, '??') AS country),
  `mozfun.norm.browser_version_info`(app_version) AS browser_version_info
FROM
  `{{ project_id }}.{{ app_name }}_derived.active_users_aggregates_v1`
WHERE
  normalized_app_name != 'Focus Android Glean'
  AND normalized_app_name != 'Focus Android Glean BrowserStack'
