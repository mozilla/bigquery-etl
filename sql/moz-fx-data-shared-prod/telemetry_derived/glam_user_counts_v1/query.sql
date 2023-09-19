WITH all_clients AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel
  FROM clients_scalar_aggregates_v1
  WHERE submission_date = @submission_date

  UNION ALL

  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel
  FROM clients_histogram_aggregates_v2
  WHERE submission_date = @submission_date
)

SELECT
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
  CAST(NULL AS STRING) as os,
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
  os,
  CAST(NULL AS INT64) AS app_version,
  app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  os,
  app_build_id,
  channel

UNION ALL

SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
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
  os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  os,
  channel

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  app_version,
  channel

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  app_version

UNION ALL

SELECT
  os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  os

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
GROUP BY
  channel

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  COUNT(DISTINCT client_id) AS total_users
FROM
  all_clients
