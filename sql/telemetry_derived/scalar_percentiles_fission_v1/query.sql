CREATE TEMP FUNCTION udf_get_values(required ARRAY<FLOAT64>, values ARRAY<FLOAT64>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  (
    SELECT ARRAY_AGG(record)
    FROM (
      SELECT
        STRUCT<key STRING, value FLOAT64>(
          CAST(k AS STRING),
          values[OFFSET(CAST(k AS INT64))]
        ) as record
      FROM
        UNNEST(required) AS k
    )
  )
);

WITH flat_clients_scalar_aggregates AS (
  SELECT * EXCEPT(scalar_aggregates)
  FROM telemetry.clients_scalar_aggregates_fission
  CROSS JOIN UNNEST(scalar_aggregates)),

percentiles AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100)  AS aggregates,
    has_fission,
  FROM
    flat_clients_scalar_aggregates
  WHERE os IS NOT NULL
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type,
    has_fissioon

  UNION ALL

  SELECT
    CAST(NULL AS STRING) as os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100)  AS aggregates,
    has_fission,
  FROM
    flat_clients_scalar_aggregates
  GROUP BY
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type,
    has_fission

  UNION ALL

  SELECT
    os,
    app_version,
    CAST(NULL AS STRING) AS app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100)  AS aggregates,
    has_fission,
  FROM
    flat_clients_scalar_aggregates
  WHERE os IS NOT NULL
  GROUP BY
    os,
    app_version,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type,
    has_fission

  UNION ALL

  SELECT
    CAST(NULL AS STRING) AS os,
    app_version,
    CAST(NULL AS STRING) AS app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100)  AS aggregates,
    has_fission,
  FROM
    flat_clients_scalar_aggregates
  GROUP BY
    app_version,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type,
    has_fission)

SELECT *
REPLACE(udf_get_values(
  [5.0, 25.0, 50.0, 75.0, 95.0],
  aggregates
) AS aggregates)
FROM percentiles
