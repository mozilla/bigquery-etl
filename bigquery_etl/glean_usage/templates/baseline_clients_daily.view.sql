{{ header }}

CREATE OR REPLACE VIEW
  `{{ daily_view }}`
AS
SELECT
  *
FROM
  `{{ daily_table }}`
