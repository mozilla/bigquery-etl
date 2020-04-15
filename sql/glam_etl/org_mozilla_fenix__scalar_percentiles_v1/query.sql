CREATE TEMP FUNCTION udf_get_values(required ARRAY<FLOAT64>, VALUES ARRAY<FLOAT64>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  (
    SELECT
      ARRAY_AGG(record)
    FROM
      (
        SELECT
          STRUCT<key STRING, value FLOAT64>(
            CAST(k AS STRING),
            VALUES
              [OFFSET(CAST(k AS INT64))]
          ) AS record
        FROM
          UNNEST(required) AS k
      )
  )
);

WITH flat_clients_scalar_aggregates AS (
  SELECT
    * EXCEPT (scalar_aggregates)
  FROM
    glam_etl.org_mozilla_fenix__clients_scalar_aggregates_v1
  CROSS JOIN
    UNNEST(scalar_aggregates)
),
percentiles AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
  FROM
    flat_clients_scalar_aggregates
  GROUP BY
    ping_type,
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
    ping_type,
    os,
    app_version,
    NULL AS app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
  FROM
    flat_clients_scalar_aggregates
  GROUP BY
    ping_type,
    os,
    app_version,
    channel,
    metric,
    metric_type,
    key,
    client_agg_type
  UNION ALL
  SELECT
    ping_type,
    NULL AS os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
  FROM
    flat_clients_scalar_aggregates
  GROUP BY
    ping_type,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    client_agg_type
  UNION ALL
  SELECT
    NULL AS ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
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
    ping_type,
    NULL AS os,
    app_version,
    NULL AS app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
  FROM
    flat_clients_scalar_aggregates
  GROUP BY
    ping_type,
    app_version,
    channel,
    metric,
    metric_type,
    key,
    client_agg_type
  UNION ALL
  SELECT
    NULL AS ping_type,
    os,
    app_version,
    NULL AS app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
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
    NULL AS ping_type,
    NULL AS os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
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
    NULL AS ping_type,
    NULL AS os,
    app_version,
    NULL AS app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    COUNT(*) AS total_users,
    APPROX_QUANTILES(value, 100) AS aggregates
  FROM
    flat_clients_scalar_aggregates
  GROUP BY
    app_version,
    channel,
    metric,
    metric_type,
    key,
    client_agg_type
)
SELECT
  * REPLACE (udf_get_values([5.0, 25.0, 50.0, 75.0, 95.0], aggregates) AS aggregates)
FROM
  percentiles
