{{ header }}

CREATE OR REPLACE VIEW
  `{{ first_seen_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ first_seen_table }}`
