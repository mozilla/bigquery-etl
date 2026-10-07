CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.stmo.object_owners`
AS
-- The user_email columns on queries_v1 and dashboards_v1 are always null, so owners come from users_v1
SELECT
  'visualization' AS object_type,
  visualizations.id AS object_id,
  users.email AS owner_email,
FROM
  `moz-fx-data-shared-prod.stmo_external.visualizations_v1` AS visualizations
INNER JOIN
  `moz-fx-data-shared-prod.stmo_external.queries_v1` AS queries
  ON queries.id = visualizations.query_id
INNER JOIN
  `moz-fx-data-shared-prod.stmo_external.users_v1` AS users
  ON users.id = queries.user_id
WHERE
  NOT queries.is_draft
  AND NOT queries.is_archived
UNION ALL
SELECT
  'dashboard' AS object_type,
  dashboards.id AS object_id,
  users.email AS owner_email,
FROM
  `moz-fx-data-shared-prod.stmo_external.dashboards_v1` AS dashboards
INNER JOIN
  `moz-fx-data-shared-prod.stmo_external.users_v1` AS users
  ON users.id = dashboards.user_id
WHERE
  NOT dashboards.is_draft
  AND NOT dashboards.is_archived
  -- The DataHub Redash ingestion denies dashboards with all-digit names
  AND NOT REGEXP_CONTAINS(dashboards.name, r'^[0-9]+$')
