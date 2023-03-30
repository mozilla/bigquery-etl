CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.active_users_aggregates`
AS
SELECT
  * EXCEPT (app_version),
  app_version,
  `mozfun.norm.browser_version_info`(app_version).major_version AS app_version_major,
  `mozfun.norm.browser_version_info`(app_version).minor_version AS app_version_minor,
  `mozfun.norm.browser_version_info`(app_version).patch_revision AS app_version_patch_revision,
  `mozfun.norm.browser_version_info`(app_version).is_major_release AS app_version_is_major_release,
  `mozfun.norm.os`(os) AS os_grouped
FROM
  `{{ project_id }}.{{ app_name }}_derived.active_users_aggregates_v1`
WHERE
  app_name NOT IN ('Focus Android Glean', 'Focus Android Glean BrowserStack')
