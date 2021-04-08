{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ daily_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ daily_table }}`
