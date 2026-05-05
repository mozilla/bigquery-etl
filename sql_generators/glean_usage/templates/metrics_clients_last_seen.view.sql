CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ target_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ target_table }}`
