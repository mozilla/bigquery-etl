CREATE OR REPLACE TABLE
  {{ dataset }}_derived.event_types_v1
AS
SELECT
  * EXCEPT (submission_date)
FROM
  {{ dataset }}_derived.event_types_history_v1

