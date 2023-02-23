{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.active_users_aggregates`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ app_name }}_derived.active_users_aggregates_v1`
