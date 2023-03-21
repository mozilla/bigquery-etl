CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_other_events_overall_v1`
AS
WITH pivot AS (
  SELECT
    window_start,
    experiment,
    branch,
    [
      STRUCT("graduated" AS event, graduate_count AS count),
      STRUCT("enroll_failed" AS event, enroll_failed_count AS count),
      STRUCT("unenroll_failed" AS event, unenroll_failed_count AS count),
      STRUCT("updated" AS event, update_count AS count),
      STRUCT("update_failed" AS event, update_failed_count AS count),
      STRUCT("disqualification" AS event, disqualification_count AS count),
      STRUCT("validation_failed" AS event, validation_failed_count AS count)
    ] AS events
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live_v1`
)
SELECT
  window_start AS `time`,
  experiment,
  branch,
  e.event AS event,
  SUM(e.count) AS value
FROM
  pivot
CROSS JOIN
  UNNEST(events) AS e
GROUP BY
  1,
  2,
  3,
  4
