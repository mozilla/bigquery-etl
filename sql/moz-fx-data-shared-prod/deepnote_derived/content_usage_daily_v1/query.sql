-- Daily usage of Deepnote projects and notebooks, from the audit log.
-- Project-level events (e.g. project_viewed, notebook_executed) have a null notebook_id
-- unless the event metadata names the notebook.
WITH snapshot_notebooks AS (
  SELECT
    notebook.notebook_id,
    project_id,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
  CROSS JOIN
    UNNEST(notebooks) AS notebook
),
audit_log_notebooks AS (
  SELECT
    JSON_VALUE(metadata, '$.notebookId') AS notebook_id,
    JSON_VALUE(metadata, '$.projectId') AS project_id,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  WHERE
    JSON_VALUE(metadata, '$.notebookId') IS NOT NULL
    AND JSON_VALUE(metadata, '$.projectId') IS NOT NULL
),
-- detached/API runs execute in a temporary copy; map executed notebooks to their source
executed_notebooks AS (
  SELECT
    JSON_VALUE(metadata, '$.executedNotebookId') AS executed_notebook_id,
    ANY_VALUE(JSON_VALUE(metadata, '$.notebookId')) AS source_notebook_id,
    ANY_VALUE(JSON_VALUE(metadata, '$.projectId')) AS source_project_id,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  WHERE
    JSON_VALUE(metadata, '$.executedNotebookId') IS NOT NULL
    AND JSON_VALUE(metadata, '$.executedNotebookId') != JSON_VALUE(metadata, '$.notebookId')
  GROUP BY
    executed_notebook_id
),
notebook_projects AS (
  SELECT
    notebook_id,
    ANY_VALUE(project_id) AS project_id,
  FROM
    (SELECT * FROM snapshot_notebooks UNION ALL SELECT * FROM audit_log_notebooks)
  GROUP BY
    notebook_id
),
events AS (
  SELECT
    DATE(created_at) AS submission_date,
    user_id,
    -- actor_type is unreliable: server-side echoes of API/MCP edits are logged as "user".
    -- Browser user agents are the Deepnote UI; everything else is the API, MCP or scripts.
    IF(STARTS_WITH(IFNULL(user_agent, ''), 'Mozilla/'), 'ui', 'api') AS channel,
    event_type,
    COALESCE(
      JSON_VALUE(metadata, '$.notebookId'),
      IF(event_type IN ('notebook_edited', 'blocks_reordered', 'runs_listed'), resource_id, NULL)
    ) AS notebook_id,
    COALESCE(
      JSON_VALUE(metadata, '$.projectId'),
      IF(
        event_type LIKE r'project\_%'
        OR event_type LIKE r'file\_%'
        OR event_type LIKE r'hardware\_%'
        OR event_type LIKE r'kernel\_%'
        OR event_type IN (
          'notebook_executed',
          'notebook_viewed',
          'published_notebook_viewed',
          'notebook_dataframe_exported',
          'integration_connected',
          'integration_disconnected',
          'static_site_published',
          'streamlit_app_created',
          'streamlit_apps_listed',
          'collaborator_invite_created',
          'collaborator_invite_edited',
          'collaborator_invite_removed',
          'cell_threads_deleted',
          'notebook_threads_deleted',
          'comment_added'
        ),
        resource_id,
        NULL
      )
    ) AS project_id,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
),
categorized AS (
  SELECT
    events.* REPLACE (
      COALESCE(
        executed_notebooks.source_project_id,
        events.project_id,
        notebook_projects.project_id
      ) AS project_id,
      COALESCE(executed_notebooks.source_notebook_id, events.notebook_id) AS notebook_id
    ),
    CASE
      WHEN event_type IN (
          'project_viewed',
          'notebook_viewed',
          'published_notebook_viewed',
          'run_viewed',
          'block_viewed'
        )
        THEN 'view'
      WHEN event_type IN (
          'notebook_edited',
          'block_created',
          'block_deleted',
          'blocks_reordered',
          'file_created',
          'file_edited',
          'file_deleted',
          'file_renamed',
          'file_uploaded'
        )
        THEN 'edit'
      WHEN event_type = 'notebook_executed'
        THEN 'execute'
      WHEN event_type IN ('hardware_started', 'hardware_restarted')
        THEN 'hardware_start'
      WHEN event_type IN ('file_exported', 'notebook_dataframe_exported', 'project_exported')
        THEN 'export'
    END AS activity,
  FROM
    events
  LEFT JOIN
    notebook_projects
    USING (notebook_id)
  LEFT JOIN
    executed_notebooks
    ON executed_notebooks.executed_notebook_id = events.notebook_id
)
SELECT
  submission_date,
  project_id,
  notebook_id,
  channel,
  COUNT(*) AS events,
  COUNTIF(activity = 'view') AS views,
  COUNTIF(activity = 'edit') AS edits,
  COUNTIF(activity = 'execute') AS executions,
  COUNTIF(activity = 'hardware_start') AS hardware_starts,
  COUNTIF(activity = 'export') AS exports,
  COUNT(DISTINCT user_id) AS active_users,
  HLL_COUNT.INIT(user_id) AS active_users_sketch,
  COUNT(DISTINCT IF(activity = 'view', user_id, NULL)) AS viewers,
  COUNT(DISTINCT IF(activity IN ('edit', 'execute'), user_id, NULL)) AS editors,
FROM
  categorized
WHERE
  project_id IS NOT NULL
  AND activity IS NOT NULL
  -- notebook_edited from non-browser clients echoes API block changes already counted
  -- as block_created/block_deleted/blocks_reordered
  AND NOT (event_type = 'notebook_edited' AND channel = 'api')
GROUP BY
  submission_date,
  project_id,
  notebook_id,
  channel
