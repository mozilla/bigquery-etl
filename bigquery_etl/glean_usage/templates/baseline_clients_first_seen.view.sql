{{ header }}

CREATE OR REPLACE VIEW
  `{{ first_seen_view }}`
AS
SELECT
  *
FROM
  `{{ first_seen_table }}`
