DECLARE submission_date DATE DEFAULT @submission_date;

EXECUTE IMMEDIATE CONCAT(
  """CREATE OR REPLACE VIEW
    telemetry.event_types
  AS
  SELECT
    * EXCEPT (submission_date)
  FROM
    telemetry_derived.event_types_v1
  WHERE
    submission_date = '""",
  CAST(submission_date AS STRING),
  "'"
);
