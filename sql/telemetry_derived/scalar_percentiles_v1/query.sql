CREATE TEMP FUNCTION udf_zip_arrays(keys ARRAY<FLOAT64>, values ARRAY<FLOAT64>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Zips two arrays into a MAP type.
  (
    SELECT ARRAY_AGG(record)
    FROM (
      SELECT
        STRUCT<key STRING, value FLOAT64>(CAST(k AS STRING), values[OFFSET(off)]) as record
      FROM
        UNNEST(keys) AS k WITH OFFSET off
    )
  )
);

WITH flat_clients_scalar_aggregates AS (
  SELECT * EXCEPT(scalar_aggregates)
  FROM clients_scalar_aggregates_v1
  CROSS JOIN UNNEST(scalar_aggregates))
  
SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) as os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS INT64) AS app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  os,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  os,
  app_version,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  os,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  app_version,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  app_version,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  os,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'percentiles' AS agg_type,
  -1 AS total_users,
  udf_zip_arrays(
    [5.0, 25.0, 50.0, 75.0, 95.0],
    [
      APPROX_QUANTILES(value, 100)[OFFSET(5)],
      APPROX_QUANTILES(value, 100)[OFFSET(25)],
      APPROX_QUANTILES(value, 100)[OFFSET(50)],
      APPROX_QUANTILES(value, 100)[OFFSET(75)],
      APPROX_QUANTILES(value, 100)[OFFSET(95)]
    ]
  ) AS aggregates
FROM
  flat_clients_scalar_aggregates
GROUP BY
  metric,
  metric_type,
  key,
  client_agg_type
