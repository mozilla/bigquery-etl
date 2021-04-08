{{ header }}

CREATE OR REPLACE VIEW
  `{{ daily_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ daily_table }}`
