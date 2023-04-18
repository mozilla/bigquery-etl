CREATE TEMP FUNCTION
  udf_get_buckets(min INT64,
    max INT64,
    num INT64,
    metric_type STRING)
  RETURNS ARRAY<INT64> AS ( (
    WITH
      buckets AS (
      SELECT
        CASE
          WHEN metric_type = 'histogram-exponential' THEN mozfun.glam.histogram_generate_exponential_buckets(min, max, num)
        ELSE
        mozfun.glam.histogram_generate_linear_buckets(min,
          max,
          num)
      END
        AS arr )
    SELECT
      ARRAY_AGG(CAST(item AS INT64))
    FROM
      buckets
    CROSS JOIN
      UNNEST(arr) AS item ) );
WITH aggregates AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    KEY,
    process,
    first_bucket,
    MAX(last_bucket) AS last_bucket,
    MAX(num_buckets) AS num_buckets,
    agg_type AS client_agg_type,
    'histogram' AS agg_type,
    CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
    mozfun.glam.histogram_fill_buckets_dirichlet( mozfun.map.sum(ARRAY_AGG(record)),
      mozfun.glam.histogram_buckets_cast_string_array(udf_get_buckets(first_bucket,
          MAX(last_bucket),
          MAX(num_buckets),
          metric_type)),
      CAST(ROUND(SUM(record.value)) AS INT64) ) AS aggregates
  FROM
      clients_histogram_bucket_counts_v1 AS bucket_counts
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    KEY,
    process,
    client_agg_type,
    first_bucket ),
  non_norm_aggregates AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    KEY,
    process,
    first_bucket,
    MAX(last_bucket) AS last_bucket,
    MAX(num_buckets) AS num_buckets,
    agg_type AS client_agg_type,
    'histogram' AS agg_type,
    mozfun.glam.histogram_fill_buckets( mozfun.map.sum(ARRAY_AGG(record)),
      mozfun.glam.histogram_buckets_cast_string_array(udf_get_buckets(first_bucket,
          MAX(last_bucket),
          MAX(num_buckets),
          metric_type))) AS non_norm_aggregates,
  FROM
    clients_non_norm_histogram_bucket_counts_v1 AS non_norm_bucket_counts
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    KEY,
    process,
    client_agg_type,
    first_bucket)

  SELECT
    IF
      (os = '*', NULL, os) AS os,
    app_version,
    IF
      (app_build_id = '*', NULL, app_build_id) AS app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    first_bucket,
    aggregates.last_bucket,
    aggregates.num_buckets,
    client_agg_type,
    agg_type,
    aggregates.total_users,
    aggregates.aggregates,
    non_norm_aggregates.non_norm_aggregates
  FROM aggregates INNER JOIN non_norm_aggregates
  USING (
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    first_bucket,
    last_bucket,
    num_buckets,
    client_agg_type,
    agg_type
  )