-- Current Deepnote projects with their recent usage, runs, apps and query cost.
WITH latest AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
  WHERE
    snapshot_date = (
      SELECT
        MAX(snapshot_date)
      FROM
        `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
    )
),
coverage AS (
  SELECT
    MIN(submission_date) AS audit_log_start_date,
    MAX(submission_date) AS audit_log_end_date,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.content_usage_daily_v1`
),
usage AS (
  SELECT
    project_id,
    MAX(IF(views > 0, submission_date, NULL)) AS last_viewed_date,
    MAX(IF(edits > 0, submission_date, NULL)) AS last_edited_date,
    MAX(IF(executions > 0, submission_date, NULL)) AS last_executed_date,
    MAX(submission_date) AS last_activity_date,
    MAX(IF(channel = 'ui', submission_date, NULL)) AS last_ui_activity_date,
    SUM(IF(submission_date > DATE_SUB(audit_log_end_date, INTERVAL 7 DAY), views, 0)) AS views_7d,
    SUM(IF(submission_date > DATE_SUB(audit_log_end_date, INTERVAL 28 DAY), views, 0)) AS views_28d,
    SUM(
      IF(
        submission_date > DATE_SUB(audit_log_end_date, INTERVAL 28 DAY)
        AND channel = 'ui',
        views,
        0
      )
    ) AS ui_views_28d,
    SUM(
      IF(
        submission_date > DATE_SUB(audit_log_end_date, INTERVAL 28 DAY)
        AND channel = 'ui',
        events,
        0
      )
    ) AS ui_events_28d,
    SUM(IF(submission_date > DATE_SUB(audit_log_end_date, INTERVAL 28 DAY), edits, 0)) AS edits_28d,
    SUM(
      IF(submission_date > DATE_SUB(audit_log_end_date, INTERVAL 28 DAY), executions, 0)
    ) AS executions_28d,
    SUM(
      IF(submission_date > DATE_SUB(audit_log_end_date, INTERVAL 28 DAY), hardware_starts, 0)
    ) AS hardware_starts_28d,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.content_usage_daily_v1`
  CROSS JOIN
    coverage
  GROUP BY
    project_id
),
-- distinct users can't be summed from the daily table
latest_notebooks AS (
  SELECT
    notebook.notebook_id,
    project_id,
  FROM
    latest
  CROSS JOIN
    UNNEST(notebooks) AS notebook
),
users AS (
  SELECT
    COALESCE(
      JSON_VALUE(metadata, '$.projectId'),
      latest_notebooks.project_id,
      resource_id
    ) AS project_id,
    COUNT(DISTINCT user_id) AS distinct_users_28d,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  LEFT JOIN
    latest_notebooks
    ON latest_notebooks.notebook_id = resource_id
  WHERE
    DATE(created_at) > DATE_SUB((SELECT audit_log_end_date FROM coverage), INTERVAL 28 DAY)
    AND event_type NOT IN ('api_key_created', 'api_key_deleted', 'api_key_viewed')
  GROUP BY
    project_id
),
runs AS (
  SELECT
    project_id,
    COUNTIF(created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)) AS runs_28d,
    COUNTIF(
      created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
      AND status IN ('error', 'internal_error')
    ) AS failed_runs_28d,
    MAX(created_at) AS last_run_at,
    ARRAY_AGG(status ORDER BY created_at DESC LIMIT 1)[OFFSET(0)] AS last_run_status,
    APPROX_QUANTILES(
      IF(
        created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY),
        TIMESTAMP_DIFF(completed_at, created_at, SECOND),
        NULL
      ),
      100
    )[OFFSET(50)] AS p50_run_seconds_28d,
    MAX(
      IF(
        created_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY),
        TIMESTAMP_DIFF(completed_at, created_at, SECOND),
        NULL
      )
    ) AS max_run_seconds_28d,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.notebook_runs_v1`
  GROUP BY
    project_id
),
queries AS (
  SELECT
    project_id,
    SUM(jobs) AS query_jobs_28d,
    SUM(failed_jobs) AS failed_query_jobs_28d,
    SUM(tib_billed) AS tib_billed_28d,
    SUM(slot_hours) AS slot_hours_28d,
    MAX(max_duration_seconds) AS max_query_seconds_28d,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.query_cost_daily_v1`
  WHERE
    project_id IS NOT NULL
    AND submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
  GROUP BY
    project_id
)
SELECT
  latest.snapshot_date,
  latest.project_id,
  latest.project_name,
  latest.project_type,
  latest.folder_path,
  latest.created_at,
  CONCAT('https://deepnote.com/project/', latest.project_id) AS project_url,
  (SELECT COUNTIF(NOT IFNULL(is_init, FALSE)) FROM UNNEST(latest.notebooks)) AS notebooks,
  ARRAY_LENGTH(latest.streamlit_apps) AS streamlit_apps,
  -- apps whose machine is stopped report "unavailable" until the next viewer starts them
  (SELECT COUNTIF(status = 'running') FROM UNNEST(latest.streamlit_apps)) AS running_streamlit_apps,
  (SELECT LOGICAL_OR(is_scheduled) FROM UNNEST(latest.notebooks)) AS is_scheduled,
  latest.machine.type_id AS machine_type_id,
  latest.machine.type_name AS machine_type_name,
  latest.machine.vcpus AS machine_vcpus,
  latest.machine.memory_gib AS machine_memory_gib,
  latest.machine.gpu_count AS machine_gpu_count,
  latest.machine.idle_shutdown_seconds AS machine_idle_shutdown_seconds,
  usage.* EXCEPT (project_id),
  IFNULL(users.distinct_users_28d, 0) AS distinct_users_28d,
  runs.* EXCEPT (project_id),
  queries.* EXCEPT (project_id),
  CASE
    WHEN usage.last_activity_date IS NULL
      THEN 'no_activity_in_audit_log'
    WHEN IFNULL(usage.ui_events_28d, 0) = 0
      AND IFNULL(usage.executions_28d, 0) = 0
      AND IFNULL(runs.runs_28d, 0) = 0
      THEN 'unused_28d'
    ELSE 'active'
  END AS usage_status,
  IFNULL(runs.last_run_status IN ('error', 'internal_error'), FALSE) AS is_broken,
  coverage.audit_log_start_date,
  coverage.audit_log_end_date,
FROM
  latest
CROSS JOIN
  coverage
LEFT JOIN
  usage
  USING (project_id)
LEFT JOIN
  users
  USING (project_id)
LEFT JOIN
  runs
  USING (project_id)
LEFT JOIN
  queries
  USING (project_id)
