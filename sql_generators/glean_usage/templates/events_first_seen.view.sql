{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.{{ events_first_seen_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ app_name }}_derived.{{ events_first_seen_table }}`
