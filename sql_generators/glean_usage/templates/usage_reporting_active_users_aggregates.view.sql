{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ usage_reporting_active_users_aggregates_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ usage_reporting_active_users_aggregates_table }}`
