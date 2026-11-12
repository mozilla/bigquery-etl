-- Projects that break or leak data with private outputs, from the latest snapshot.
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
-- one-way switch
private_projects AS (
  SELECT
    COALESCE(JSON_VALUE(metadata, '$.projectId'), resource_id) AS project_id,
    MIN(created_at) AS private_since,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  WHERE
    event_type = 'project_private_execution_enabled'
  GROUP BY
    project_id
),
notebook_projects AS (
  SELECT
    notebook.notebook_id,
    project_id,
  FROM
    latest
  CROSS JOIN
    UNNEST(notebooks) AS notebook
),
-- people who only viewed a project in the last 28 days
project_activity AS (
  SELECT
    COALESCE(
      JSON_VALUE(audit_log.metadata, '$.projectId'),
      notebook_projects.project_id,
      audit_log.resource_id
    ) AS project_id,
    audit_log.user_id,
    LOGICAL_OR(
      audit_log.event_type IN (
        'project_viewed',
        'notebook_viewed',
        'published_notebook_viewed',
        'block_viewed',
        'run_viewed'
      )
    ) AS viewed,
    LOGICAL_OR(
      audit_log.event_type IN (
        'notebook_edited',
        'block_created',
        'block_deleted',
        'blocks_reordered',
        'notebook_executed',
        'file_created',
        'file_edited'
      )
    ) AS worked,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1` AS audit_log
  LEFT JOIN
    notebook_projects
    ON notebook_projects.notebook_id = audit_log.resource_id
  WHERE
    DATE(audit_log.created_at) > DATE_SUB((SELECT MAX(snapshot_date) FROM latest), INTERVAL 28 DAY)
    AND audit_log.user_id IS NOT NULL
  GROUP BY
    project_id,
    user_id
),
viewers AS (
  SELECT
    project_id,
    COUNTIF(viewed AND NOT worked) AS viewer_only_users,
  FROM
    project_activity
  WHERE
    project_id IS NOT NULL
  GROUP BY
    project_id
),
flags AS (
  -- caches shared between viewers of apps that use the viewer's OAuth (all projects)
  SELECT
    project_id,
    'streamlit_shared_cache' AS flag,
    FORMAT(
      '%s: %d cache decorator(s) without scope="session"',
      app.entrypoint,
      app.unscoped_cache_count
    ) AS detail,
  FROM
    latest
  CROSS JOIN
    UNNEST(streamlit_apps) AS app
  WHERE
    app.unscoped_cache_count > 0
    AND NOT app.uses_service_account
  UNION ALL
  -- session-scoped caches need Streamlit 1.54+
  SELECT
    project_id,
    'streamlit_before_1_54',
    FORMAT('%s: streamlit%s', streamlit_requirement.file, streamlit_requirement.specifier),
  FROM
    latest
  WHERE
    streamlit_requirement.excludes_session_cache
  UNION ALL
  -- files are shared with all collaborators
  SELECT
    project_id,
    'saved_data_file',
    file,
  FROM
    latest
  CROSS JOIN
    UNNEST(files) AS file
  WHERE
    project_id IN (SELECT project_id FROM private_projects)
    AND REGEXP_CONTAINS(
      LOWER(file),
      r'\.(csv|tsv|parquet|xlsx?|feather|arrow|pkl|pickle|h5|hdf5|sqlite3?|db|jsonl|ndjson)$'
    )
  UNION ALL
  -- viewers see empty notebooks
  SELECT
    project_id,
    'viewer_audience',
    FORMAT('%d people only viewed the project in the last 28 days', viewer_only_users),
  FROM
    viewers
  WHERE
    viewer_only_users > 0
    AND project_id IN (SELECT project_id FROM private_projects)
  UNION ALL
  -- scheduled results and run history are only visible to their owner
  SELECT
    project_id,
    'runs_not_visible',
    FORMAT('%d scheduled notebook(s)', (SELECT COUNTIF(is_scheduled) FROM UNNEST(notebooks))),
  FROM
    latest
  WHERE
    project_id IN (SELECT project_id FROM private_projects)
    AND EXISTS (SELECT 1 FROM UNNEST(notebooks) WHERE is_scheduled)
  UNION ALL
  SELECT
    project_id,
    'schedule_access_revoked',
    notebook.name,
  FROM
    latest
  CROSS JOIN
    UNNEST(notebooks) AS notebook
  WHERE
    notebook.schedule.pause_reason = 'access-revoked'
)
SELECT
  latest.snapshot_date,
  latest.workspace_id,
  latest.workspace_name,
  latest.project_id,
  latest.project_name,
  CONCAT('https://deepnote.com/project/', latest.project_id) AS project_url,
  private_projects.project_id IS NOT NULL AS is_private,
  private_projects.private_since,
  flags.flag,
  flags.detail,
FROM
  flags
JOIN
  latest
  USING (project_id)
LEFT JOIN
  private_projects
  USING (project_id)
