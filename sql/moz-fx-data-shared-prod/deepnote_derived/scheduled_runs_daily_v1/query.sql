-- Daily notebook runs by trigger, with outcome and duration.
-- The API doesn't report what triggered a run; a run of a scheduled notebook counts as
-- scheduled if it started within 5 minutes after one of the schedule's fire times.
WITH notebooks AS (
  SELECT
    notebook.notebook_id,
    ARRAY_AGG(
      STRUCT(project_id, project_name, notebook.name AS notebook_name)
      ORDER BY
        snapshot_date DESC
      LIMIT
        1
    )[OFFSET(0)].*,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
  CROSS JOIN
    UNNEST(notebooks) AS notebook
  GROUP BY
    notebook_id
),
runs AS (
  SELECT
    runs.*,
    schedules.notebook_id IS NOT NULL AS is_scheduled_notebook,
    IFNULL(
      EXISTS (
        SELECT
          1
        FROM
          UNNEST(schedules.fire_times) AS fire_time
        CROSS JOIN
          UNNEST(GENERATE_ARRAY(0, 5)) AS minutes_late
        WHERE
          fire_time.day_of_week = EXTRACT(
            DAYOFWEEK
            FROM
              DATETIME_SUB(
                DATETIME(runs.created_at, schedules.timezone),
                INTERVAL minutes_late MINUTE
              )
          ) - 1
          AND fire_time.minute_of_day = (
            EXTRACT(
              HOUR
              FROM
                DATETIME_SUB(
                  DATETIME(runs.created_at, schedules.timezone),
                  INTERVAL minutes_late MINUTE
                )
            ) * 60 + EXTRACT(
              MINUTE
              FROM
                DATETIME_SUB(
                  DATETIME(runs.created_at, schedules.timezone),
                  INTERVAL minutes_late MINUTE
                )
            )
          )
      ),
      FALSE
    ) AS is_scheduled_run,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.notebook_runs_v1` AS runs
  LEFT JOIN
    `moz-fx-data-shared-prod.deepnote_derived.notebook_schedules_v1` AS schedules
    USING (notebook_id)
)
SELECT
  DATE(runs.created_at) AS submission_date,
  FORMAT_DATE('%A', DATE(runs.created_at)) AS day_of_week,
  runs.project_id,
  notebooks.project_name,
  runs.notebook_id,
  notebooks.notebook_name,
  runs.is_scheduled_notebook,
  IF(runs.is_scheduled_run, 'scheduled', 'api_or_manual') AS trigger,
  COUNT(*) AS runs,
  COUNTIF(runs.status = 'success') AS successful_runs,
  COUNTIF(runs.status IN ('error', 'internal_error')) AS failed_runs,
  COUNTIF(runs.status = 'stopped') AS stopped_runs,
  COUNTIF(runs.status IN ('pending', 'running')) AS unfinished_runs,
  ARRAY_AGG(runs.status ORDER BY runs.created_at DESC LIMIT 1)[OFFSET(0)] AS last_run_status,
  MAX(runs.created_at) AS last_run_at,
  AVG(
    TIMESTAMP_DIFF(runs.completed_at, runs.created_at, MILLISECOND)
  ) / 1000 AS avg_duration_seconds,
  MAX(
    TIMESTAMP_DIFF(runs.completed_at, runs.created_at, MILLISECOND)
  ) / 1000 AS max_duration_seconds,
FROM
  runs
LEFT JOIN
  notebooks
  USING (notebook_id)
GROUP BY
  submission_date,
  day_of_week,
  project_id,
  project_name,
  notebook_id,
  notebook_name,
  is_scheduled_notebook,
  trigger
