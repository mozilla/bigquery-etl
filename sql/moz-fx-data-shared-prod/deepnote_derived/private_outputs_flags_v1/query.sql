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
private_projects AS (
  SELECT
    project_id,
    private_outputs_enabled_at AS private_since,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.content_inventory_v1`
  WHERE
    has_private_outputs
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
