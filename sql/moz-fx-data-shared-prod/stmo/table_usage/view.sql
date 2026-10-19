CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stmo.table_usage`
AS
WITH jobs AS (
  SELECT DISTINCT
    reference_project_id,
    reference_dataset_id,
    reference_table_id,
    SAFE_CAST(query_id AS INT64) AS query_id,
    IF(is_scheduled, NULL, username) AS username,
    submission_date,
  FROM
    `moz-fx-data-shared-prod.monitoring.bigquery_usage`
  WHERE
    submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND user_type = 'redash'
    AND error_reason IS NULL
    AND reference_table_id IS NOT NULL
    -- Temporary tables created by multi-statement queries
    AND NOT STARTS_WITH(reference_dataset_id, '_script')
),
query_usage AS (
  SELECT
    jobs.reference_project_id,
    jobs.reference_dataset_id,
    jobs.reference_table_id,
    jobs.query_id,
    ANY_VALUE(queries.name) AS query_name,
    ANY_VALUE(query_owners.email) AS query_owner_email,
    -- Null for deleted queries. Archived queries and expired schedules don't run.
    ANY_VALUE(
      IF(
        queries.id IS NULL,
        NULL,
        NOT queries.is_archived
        AND JSON_VALUE(queries.schedule, '$.interval') IS NOT NULL
        AND COALESCE(
          SAFE_CAST(JSON_VALUE(queries.schedule, '$.until') AS DATE) >= CURRENT_DATE(),
          TRUE
        )
      )
    ) AS is_scheduled,
    COUNT(DISTINCT jobs.username) AS users,
    MAX(jobs.submission_date) AS last_run_date,
  FROM
    jobs
  -- Left join so deleted queries are still listed
  LEFT JOIN
    `moz-fx-data-shared-prod.stmo_external.queries_v1` AS queries
    ON queries.id = jobs.query_id
  LEFT JOIN
    `moz-fx-data-shared-prod.stmo_external.users_v1` AS query_owners
    ON query_owners.id = queries.user_id
  WHERE
    jobs.query_id IS NOT NULL
  GROUP BY
    reference_project_id,
    reference_dataset_id,
    reference_table_id,
    query_id
),
dashboard_views AS (
  SELECT
    object_id AS dashboard_id,
    SUM(views) AS views,
    COUNT(DISTINCT user_email) AS users,
  FROM
    `moz-fx-data-shared-prod.stmo.object_views_daily`
  WHERE
    object_type = 'dashboard'
    AND submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY
    dashboard_id
),
table_dashboards AS (
  SELECT DISTINCT
    query_usage.reference_project_id,
    query_usage.reference_dataset_id,
    query_usage.reference_table_id,
    query_dashboards.dashboard_id,
    query_dashboards.dashboard_name,
    query_dashboards.dashboard_url,
    query_dashboards.dashboard_owner_email,
    COALESCE(dashboard_views.views, 0) AS views,
    COALESCE(dashboard_views.users, 0) AS users,
  FROM
    query_usage
  INNER JOIN
    `moz-fx-data-shared-prod.stmo.query_dashboards` AS query_dashboards
    USING (query_id)
  LEFT JOIN
    dashboard_views
    USING (dashboard_id)
  WHERE
    NOT query_dashboards.dashboard_is_archived
),
table_usage AS (
  SELECT
    reference_project_id,
    reference_dataset_id,
    reference_table_id,
    COUNT(DISTINCT query_id) AS queries,
    COUNT(DISTINCT username) AS users,
    MAX(submission_date) AS last_used_date,
  FROM
    jobs
  GROUP BY
    reference_project_id,
    reference_dataset_id,
    reference_table_id
),
query_lists AS (
  SELECT
    reference_project_id,
    reference_dataset_id,
    reference_table_id,
    ARRAY_AGG(
      STRUCT(
        query_id,
        query_name,
        'https://sql.telemetry.mozilla.org/queries/' || query_id AS query_url,
        query_owner_email,
        is_scheduled,
        users,
        last_run_date
      )
      ORDER BY
        last_run_date DESC,
        users DESC
    ) AS query_list,
  FROM
    query_usage
  GROUP BY
    reference_project_id,
    reference_dataset_id,
    reference_table_id
),
dashboard_lists AS (
  SELECT
    reference_project_id,
    reference_dataset_id,
    reference_table_id,
    ARRAY_AGG(
      STRUCT(dashboard_id, dashboard_name, dashboard_url, dashboard_owner_email, views, users)
      ORDER BY
        views DESC
    ) AS dashboard_list,
  FROM
    table_dashboards
  GROUP BY
    reference_project_id,
    reference_dataset_id,
    reference_table_id
)
SELECT
  table_usage.*,
  COALESCE(query_lists.query_list, []) AS query_list,
  COALESCE(dashboard_lists.dashboard_list, []) AS dashboard_list,
FROM
  table_usage
LEFT JOIN
  query_lists
  USING (reference_project_id, reference_dataset_id, reference_table_id)
LEFT JOIN
  dashboard_lists
  USING (reference_project_id, reference_dataset_id, reference_table_id)
