{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  *,
  CASE
    WHEN first_seen_date = submission_date
      THEN 'new_profile'
    WHEN DATE_DIFF(submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
FROM
  `{{ project_id }}.{{ dataset }}_derived.{{ name }}_{{ version }}`
