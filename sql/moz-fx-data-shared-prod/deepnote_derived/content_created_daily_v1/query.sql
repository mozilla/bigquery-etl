-- Content that currently exists in the default Deepnote workspace (latest workspace
-- snapshot), counted on the day it was created, by content type.
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
first_published AS (
  SELECT
    IF(event_type = 'notebook_published', JSON_VALUE(metadata, '$.notebookId'), resource_id) AS id,
    event_type,
    MIN(created_at) AS published_at,
  FROM
    `moz-fx-data-shared-prod.deepnote_derived.audit_log_v1`
  WHERE
    event_type IN ('notebook_published', 'static_site_published')
  GROUP BY
    id,
    event_type
),
items AS (
  SELECT
    'notebook' AS content_type,
    notebook.created_at,
  FROM
    latest
  CROSS JOIN
    UNNEST(notebooks) AS notebook
  WHERE
    NOT IFNULL(notebook.is_init, FALSE)
  UNION ALL
  SELECT
    'streamlit_app',
    app.created_at,
  FROM
    latest
  CROSS JOIN
    UNNEST(streamlit_apps) AS app
  UNION ALL
  -- native apps: existing notebooks that were published as Deepnote apps
  SELECT
    'native_app',
    first_published.published_at,
  FROM
    latest
  CROSS JOIN
    UNNEST(notebooks) AS notebook
  JOIN
    first_published
    ON first_published.id = notebook.notebook_id
    AND first_published.event_type = 'notebook_published'
  UNION ALL
  -- analyses started with the mozdata plugin's /mozdata:ask, which adds an "Exploration log" notebook
  SELECT
    'ask_analysis',
    latest.created_at,
  FROM
    latest
  WHERE
    EXISTS (SELECT 1 FROM UNNEST(notebooks) WHERE name = 'Exploration log')
  UNION ALL
  -- analyses packaged with /mozdata:package, which publishes _deepnote_static/report.html
  SELECT
    'packaged_analysis',
    COALESCE(first_published.published_at, latest.created_at),
  FROM
    latest
  LEFT JOIN
    first_published
    ON first_published.id = latest.project_id
    AND first_published.event_type = 'static_site_published'
  WHERE
    '_deepnote_static/report.html' IN UNNEST(latest.files)
  UNION ALL
  -- dashboards built with the mozdata plugin's dashboard skill: a Streamlit app serving app.py
  -- in a project that also holds the skill's metadata.yaml
  SELECT
    'dashboard_skill_app',
    app.created_at,
  FROM
    latest
  CROSS JOIN
    UNNEST(streamlit_apps) AS app
  WHERE
    app.entrypoint = 'app.py'
    AND 'metadata.yaml' IN UNNEST(latest.files)
)
SELECT
  DATE(created_at) AS submission_date,
  content_type,
  COUNT(*) AS created,
FROM
  items
GROUP BY
  submission_date,
  content_type
