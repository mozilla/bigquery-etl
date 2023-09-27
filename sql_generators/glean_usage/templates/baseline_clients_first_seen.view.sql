{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ first_seen_view }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ daily_table }}`
WHERE
  is_new_profile
  AND submission_date > "2010-01-01"
