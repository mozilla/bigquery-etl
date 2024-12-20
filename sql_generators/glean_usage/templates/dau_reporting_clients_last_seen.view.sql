{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dau_reporting_clients_last_seen_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ dau_reporting_clients_last_seen_table }}`
