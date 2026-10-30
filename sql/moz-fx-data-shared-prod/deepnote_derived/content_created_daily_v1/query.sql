-- Content created per day in the default Deepnote workspace, by content type.
-- Each item is counted once, on the day it was first created; deletions are ignored.
WITH audit_log AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
),
-- excludes the temporary project copies that detached and scheduled runs execute in
real_projects AS (
  SELECT
    resource_id AS project_id
  FROM
    audit_log
  WHERE
    event_type = 'project_created'
  UNION DISTINCT
  SELECT
    project_id
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.workspace_snapshot_v1`
),
items AS (
  -- notebooks
  SELECT
    'notebook' AS content_type,
    CONCAT(
      resource_id,
      '/',
      JSON_VALUE(metadata, '$.path'),
      '/',
      CAST(created_at AS STRING)
    ) AS item_id,
    created_at,
  FROM
    audit_log
  WHERE
    event_type = 'file_created'
    AND JSON_VALUE(metadata, '$.type') = 'notebook'
    AND resource_id IN (SELECT project_id FROM real_projects)
  UNION ALL
  -- Streamlit apps; apps created in the UI log no app ID, so identify them by project and entrypoint
  SELECT
    'streamlit_app',
    CONCAT(resource_id, '/', IFNULL(JSON_VALUE(metadata, '$.entrypoint'), '')),
    created_at,
  FROM
    audit_log
  WHERE
    event_type = 'streamlit_app_created'
    AND resource_id IN (SELECT project_id FROM real_projects)
  UNION ALL
  -- native apps: notebooks published as Deepnote apps
  SELECT
    'native_app',
    JSON_VALUE(metadata, '$.notebookId'),
    created_at,
  FROM
    audit_log
  WHERE
    event_type = 'notebook_published'
  UNION ALL
  -- analyses started with the mozdata plugin's /mozdata:ask, which creates an "Exploration log" notebook
  SELECT
    'ask_analysis',
    resource_id,
    created_at,
  FROM
    audit_log
  WHERE
    (
      (event_type = 'file_renamed' AND JSON_VALUE(metadata, '$.to') = 'Exploration log')
      OR (event_type = 'file_created' AND JSON_VALUE(metadata, '$.path') = 'Exploration log')
    )
    AND resource_id IN (SELECT project_id FROM real_projects)
  UNION ALL
  -- analyses packaged with /mozdata:package, which publishes _deepnote_static/report.html as a static site
  SELECT
    'packaged_analysis',
    resource_id,
    created_at,
  FROM
    audit_log
  WHERE
    event_type = 'static_site_published'
    AND resource_id IN (
      SELECT
        resource_id
      FROM
        audit_log
      WHERE
        event_type IN ('file_created', 'file_edited', 'file_uploaded')
        AND JSON_VALUE(metadata, '$.path') = '_deepnote_static/report.html'
    )
),
first_created AS (
  SELECT
    content_type,
    item_id,
    MIN(created_at) AS created_at,
  FROM
    items
  GROUP BY
    content_type,
    item_id
)
SELECT
  DATE(created_at) AS submission_date,
  content_type,
  COUNT(*) AS created,
FROM
  first_created
GROUP BY
  submission_date,
  content_type
