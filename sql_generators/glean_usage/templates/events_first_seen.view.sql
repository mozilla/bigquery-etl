{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ events_first_seen_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ events_first_seen_table }}`
