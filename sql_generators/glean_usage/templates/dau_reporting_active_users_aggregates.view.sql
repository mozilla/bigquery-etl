{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dau_reporting_active_users_aggregates_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ dau_reporting_active_users_aggregates_table }}`
