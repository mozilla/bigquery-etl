{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dau_reporting_clients_first_seen_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ dau_reporting_clients_first_seen_table }}`
