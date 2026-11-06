-- Daily BigQuery usage of the default Deepnote workspace by project and notebook.
-- Detached, API and scheduled runs execute in a temporary copy of the project, so the
-- query comment names the copy. Resolve copies to the source project/notebook via the
-- audit log (executedNotebookId -> notebookId) and, for scheduled runs, via the run window.
WITH executed_notebooks AS (
  SELECT
    JSON_VALUE(metadata, '$.executedNotebookId') AS executed_notebook_id,
    ANY_VALUE(JSON_VALUE(metadata, '$.notebookId')) AS notebook_id,
    ANY_VALUE(JSON_VALUE(metadata, '$.projectId')) AS project_id,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  WHERE
    JSON_VALUE(metadata, '$.executedNotebookId') IS NOT NULL
  GROUP BY
    executed_notebook_id
),
jobs AS (
  SELECT
    jobs.* EXCEPT (deepnote_project_id, deepnote_notebook_id),
    COALESCE(
      executed_notebooks.project_id,
      runs.project_id,
      jobs.deepnote_project_id
    ) AS deepnote_project_id,
    COALESCE(
      executed_notebooks.notebook_id,
      runs.notebook_id,
      jobs.deepnote_notebook_id
    ) AS deepnote_notebook_id,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.query_jobs_v1` AS jobs
  LEFT JOIN
    executed_notebooks
    ON executed_notebooks.executed_notebook_id = jobs.deepnote_notebook_id
  LEFT JOIN
    `moz-fx-data-shared-prod.deepnote_derived.notebook_runs_v1` AS runs
    ON DATE(runs.created_at) = jobs.submission_date
    AND jobs.submitted_by = 'scheduling'
    AND jobs.creation_time
    BETWEEN runs.created_at
    AND IFNULL(runs.completed_at, TIMESTAMP_ADD(runs.created_at, INTERVAL 6 HOUR))
  WHERE
    TRUE
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY jobs.job_id ORDER BY runs.created_at DESC) = 1
),
notebooks AS (
  SELECT
    notebook.notebook_id,
    ARRAY_AGG(notebook.name ORDER BY snapshot_date DESC LIMIT 1)[OFFSET(0)] AS notebook_name,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
  CROSS JOIN
    UNNEST(notebooks) AS notebook
  GROUP BY
    notebook_id
),
snapshot_projects AS (
  SELECT
    project_id,
    ARRAY_AGG(project_name ORDER BY snapshot_date DESC LIMIT 1)[OFFSET(0)] AS project_name,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
  GROUP BY
    project_id
),
-- names of projects that never appeared in a snapshot, e.g. deleted before snapshots began
audit_log_projects AS (
  SELECT
    resource_id AS project_id,
    ARRAY_AGG(
      IF(event_type = 'project_renamed', JSON_VALUE(metadata, '$.to'), NULL) IGNORE NULLS
      ORDER BY
        created_at DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS project_name,
    LOGICAL_OR(event_type = 'project_deleted') AS is_deleted,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  WHERE
    event_type IN ('project_renamed', 'project_deleted')
  GROUP BY
    project_id
),
projects AS (
  SELECT
    project_id,
    COALESCE(
      snapshot_projects.project_name,
      audit_log_projects.project_name,
      IF(audit_log_projects.is_deleted, '(deleted project)', '(unknown project)')
    ) AS project_name,
  FROM
    snapshot_projects
  FULL OUTER JOIN
    audit_log_projects
    USING (project_id)
)
SELECT
  submission_date,
  query_source,
  submitted_by,
  gcp_project_id,
  deepnote_project_id AS project_id,
  IF(
    deepnote_project_id IS NULL,
    NULL,
    COALESCE(
      projects.project_name,
      IF(
        submitted_by IN ('detached', 'api', 'scheduling'),
        '(unattributed run copy)',
        '(unknown project)'
      )
    )
  ) AS project_name,
  deepnote_notebook_id AS notebook_id,
  notebooks.notebook_name,
  COUNT(*) AS jobs,
  COUNTIF(error_reason IS NOT NULL) AS failed_jobs,
  COUNTIF(cache_hit) AS cached_jobs,
  SUM(total_bytes_processed) / POW(1024, 4) AS tib_processed,
  SUM(total_bytes_billed) / POW(1024, 4) AS tib_billed,
  SUM(total_slot_ms) / 1000 / 3600 AS slot_hours,
  -- list-price cost estimate, same formula as moz-fx-data-shared-prod.monitoring.bigquery_usage
  SUM(
    CASE
      WHEN reservation_id IS NULL
        AND total_bytes_billed IS NOT NULL
        THEN total_bytes_billed / POW(1024, 4) * 6.25
      WHEN reservation_id = 'default-pipeline'
        THEN 0
      WHEN reservation_id LIKE '%.metadata-generation'
        OR reservation_id LIKE '%.shredder-all'
        OR reservation_id LIKE '%.shredder-telemetry-main'
        OR reservation_id LIKE '%.shredder-desktop-metrics'
        THEN total_slot_ms / (60 * 60 * 1000) * 0.048
      ELSE total_slot_ms / (60 * 60 * 1000) * 0.06
    END
  ) AS cost_usd,
  APPROX_QUANTILES(duration_ms, 100)[OFFSET(50)] / 1000 AS p50_duration_seconds,
  APPROX_QUANTILES(duration_ms, 100)[OFFSET(90)] / 1000 AS p90_duration_seconds,
  MAX(duration_ms) / 1000 AS max_duration_seconds,
FROM
  jobs
LEFT JOIN
  projects
  ON projects.project_id = deepnote_project_id
LEFT JOIN
  notebooks
  ON notebooks.notebook_id = deepnote_notebook_id
GROUP BY
  submission_date,
  query_source,
  submitted_by,
  gcp_project_id,
  project_id,
  project_name,
  notebook_id,
  notebook_name
