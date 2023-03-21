--- User-facing view for all mobile apps. Generated via sql_generators.active_users_aggregates.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset_id }}.active_users_aggregates_mobile`
AS
SELECT
  {{ view_columns }}
FROM
  `{{ project_id }}.{{ fenix_dataset }}_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  { {view_columns } }
FROM
  `{{ project_id }}.{{ firefox_ios_dataset }}_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  { {view_columns } }
FROM
  `{{ project_id }}.{{ focus_ios_dataset }}_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  { {view_columns } }
FROM
  `{{ project_id }}.{{ klar_ios_dataset }}_derived.active_users_aggregates_v1`
UNION ALL
SELECT
  { {view_columns } }
FROM
  `{{ project_id }}.{{ focus_android_dataset }}_derived.active_users_aggregates_v1`
WHERE
  app_name NOT IN ('Focus Android Glean', 'Focus Android Glean BrowserStack')
