{{ header }}
{% include "metric_bucketing_v1.udf.sql" %}

WITH bucketed_booleans AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    udf_boolean_buckets(scalar_aggregates) AS scalar_aggregates
  FROM
    clients_scalar_aggregates_v1
),
bucketed_scalars AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    SAFE_CAST(udf_bucket(SAFE_CAST(value AS FLOAT64)) AS STRING) AS bucket
  FROM
    clients_scalar_aggregates_v1
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE
    metric_type = 'scalar'
    OR metric_type = 'keyed-scalar'
),
booleans_and_scalars AS (
  SELECT
    * EXCEPT (scalar_aggregates)
  FROM
    bucketed_booleans
  CROSS JOIN
    UNNEST(scalar_aggregates)
  UNION ALL
  SELECT
    *
  FROM
    bucketed_scalars
)
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
  'histogram' AS agg_type,
  bucket,
  COUNT(*) AS count
FROM
  booleans_and_scalars
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
  bucket
