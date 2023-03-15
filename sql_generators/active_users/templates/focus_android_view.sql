CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.active_users_aggregates`
AS
SELECT
  *,
  `mozfun.norm.os`(os) AS os_grouped
FROM
  `{{ project_id }}.{{ app_name }}_derived.active_users_aggregates_v1`
WHERE
  app_name != 'Focus Android Glean'
  AND app_name != 'Focus Android Glean BrowserStack'
