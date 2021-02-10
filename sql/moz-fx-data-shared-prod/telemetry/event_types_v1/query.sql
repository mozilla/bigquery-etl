CREATE OR REPLACE TABLE
  telemetry.event_types_v1
AS
SELECT
  * EXCEPT (submission_date)
FROM
  telemetry_derived.event_types_history_v1
WHERE
  submission_date = @submission_date
