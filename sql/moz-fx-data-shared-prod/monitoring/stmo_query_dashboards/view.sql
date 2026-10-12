CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.stmo_query_dashboards`
AS
SELECT
  queries.id AS query_id,
  queries.name AS query_name,
  queries.is_archived AS query_is_archived,
  dashboards.id AS dashboard_id,
  dashboards.name AS dashboard_name,
  'https://sql.telemetry.mozilla.org/dashboard/' || dashboards.slug AS dashboard_url,
  dashboard_owners.email AS dashboard_owner_email,
  dashboards.is_archived AS dashboard_is_archived,
  dashboards.is_draft AS dashboard_is_draft,
  COUNT(*) AS widget_count,
FROM
  `moz-fx-data-shared-prod.stmo_external.widgets_v1` AS widgets
INNER JOIN
  `moz-fx-data-shared-prod.stmo_external.visualizations_v1` AS visualizations
  ON visualizations.id = widgets.visualization_id
INNER JOIN
  `moz-fx-data-shared-prod.stmo_external.queries_v1` AS queries
  ON queries.id = visualizations.query_id
INNER JOIN
  `moz-fx-data-shared-prod.stmo_external.dashboards_v1` AS dashboards
  ON dashboards.id = widgets.dashboard_id
LEFT JOIN
  `moz-fx-data-shared-prod.stmo_external.users_v1` AS dashboard_owners
  ON dashboard_owners.id = dashboards.user_id
GROUP BY
  query_id,
  query_name,
  query_is_archived,
  dashboard_id,
  dashboard_name,
  dashboard_url,
  dashboard_owner_email,
  dashboard_is_archived,
  dashboard_is_draft
