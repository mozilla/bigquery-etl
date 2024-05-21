--- User-facing view for all mobile apps. Generated via sql_generators.active_users.
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset_id }}.active_users_aggregates_device`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ fenix_dataset }}.active_users_aggregates_device`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.{{ firefox_ios_dataset }}.active_users_aggregates_device`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.{{ focus_android_dataset }}.active_users_aggregates_device`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.{{ focus_ios_dataset }}.active_users_aggregates_device`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.{{ klar_android_dataset }}.active_users_aggregates_device`
UNION ALL
SELECT
  *
FROM
  `{{ project_id }}.{{ klar_ios_dataset }}.active_users_aggregates_device`
