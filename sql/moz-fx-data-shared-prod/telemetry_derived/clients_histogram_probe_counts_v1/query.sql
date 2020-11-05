CREATE TEMP FUNCTION udf_get_buckets(min INT64, max INT64, num INT64, metric_type STRING)
RETURNS ARRAY<INT64> AS (
  (
    WITH buckets AS (
      SELECT
        CASE
          WHEN metric_type = 'histogram-exponential'
          THEN mozfun.glam.histogram_generate_exponential_buckets(min, max, num)
          ELSE mozfun.glam.histogram_generate_linear_buckets(min, max, num)
       END AS arr
    )

    SELECT ARRAY_AGG(CAST(item AS INT64))
    FROM buckets
    CROSS JOIN UNNEST(arr) AS item
  )
);

SELECT
  IF(os = '*', NULL, os) AS os,
  app_version,
  IF(app_build_id = '*', NULL, app_build_id) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  process,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  mozfun.glam.histogram_fill_buckets_dirichlet(
    mozfun.map.sum(ARRAY_AGG(record)),
    mozfun.glam.histogram_buckets_cast_string_array(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type)),
    CAST(ROUND(SUM(record.value)) AS INT64)
  ) AS aggregates
FROM clients_histogram_bucket_counts_v1
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
  first_bucket,
  last_bucket,
  num_buckets
