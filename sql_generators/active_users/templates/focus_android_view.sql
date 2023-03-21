CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.active_users_aggregates`
AS
SELECT
  { {view_columns } }
FROM
  `{{ project_id }}.{{ app_name }}_derived.active_users_aggregates_v1`
WHERE
  app_name NOT IN ('Focus Android Glean', 'Focus Android Glean BrowserStack')
