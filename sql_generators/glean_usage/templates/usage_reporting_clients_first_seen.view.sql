{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ usage_reporting_clients_first_seen_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ usage_reporting_clients_first_seen_table }}`
