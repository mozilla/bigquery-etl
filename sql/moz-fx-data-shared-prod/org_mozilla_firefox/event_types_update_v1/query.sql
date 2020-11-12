DECLARE submission_date DATE DEFAULT @submission_date;

EXECUTE IMMEDIATE CONCAT(
  """CREATE OR REPLACE VIEW
    org_mozilla_firefox.event_types
  AS
  SELECT
    * EXCEPT (submission_date)
  FROM
    org_mozilla_firefox_derived.event_types_v1
  WHERE
    submission_date = '""",
  CAST(submission_date AS STRING),
  "'"
);
