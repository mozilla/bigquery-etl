CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.glam_etl.org_mozilla_fenix_view_user_counts_v1`
AS
WITH all_clients AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_clients_scalar_aggregates_v1
  UNION ALL
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel
  FROM
    `moz-fx-data-shared-prod`.glam_etl.org_mozilla_fenix_clients_histogram_aggregates_v1
)
SELECT
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  ping_type,
  os,
  app_version,
  app_build_id,
  channel
UNION ALL
SELECT
  ping_type,
  os,
  app_version,
  NULL AS app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  ping_type,
  os,
  app_version,
  channel
UNION ALL
SELECT
  ping_type,
  NULL AS os,
  app_version,
  app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  ping_type,
  app_version,
  app_build_id,
  channel
UNION ALL
SELECT
  NULL AS ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  os,
  app_version,
  app_build_id,
  channel
UNION ALL
SELECT
  ping_type,
  NULL AS os,
  app_version,
  NULL AS app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  ping_type,
  app_version,
  channel
UNION ALL
SELECT
  NULL AS ping_type,
  os,
  app_version,
  NULL AS app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  os,
  app_version,
  channel
UNION ALL
SELECT
  NULL AS ping_type,
  NULL AS os,
  app_version,
  app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  app_version,
  app_build_id,
  channel
UNION ALL
SELECT
  NULL AS ping_type,
  NULL AS os,
  app_version,
  NULL AS app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  app_version,
  channel
