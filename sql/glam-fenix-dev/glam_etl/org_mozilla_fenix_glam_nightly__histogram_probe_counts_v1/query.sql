-- query for org_mozilla_fenix_glam_nightly__histogram_probe_counts_v1;
CREATE TEMP FUNCTION udf_get_buckets(
  metric_type STRING,
  range_min INT64,
  range_max INT64,
  bucket_count INT64
)
RETURNS ARRAY<INT64> AS (
  (
    WITH buckets AS (
      SELECT
        CASE
        WHEN
          metric_type = 'timing_distribution'
        THEN
          -- https://mozilla.github.io/glean/book/user/metrics/timing_distribution.html
          mozfun.glam.histogram_generate_functional_buckets(2, 8, range_max)
        WHEN
          metric_type = 'memory_distribution'
        THEN
          -- https://mozilla.github.io/glean/book/user/metrics/memory_distribution.html
          mozfun.glam.histogram_generate_functional_buckets(2, 16, range_max)
        WHEN
          metric_type = 'custom_distribution_exponential'
        THEN
          mozfun.glam.histogram_generate_exponential_buckets(range_min, range_max, bucket_count)
        WHEN
          metric_type = 'custom_distribution_linear'
        THEN
          mozfun.glam.histogram_generate_linear_buckets(range_min, range_max, bucket_count)
        ELSE
          []
        END
        AS arr
    )
    SELECT
      ARRAY_AGG(CAST(item AS INT64))
    FROM
      buckets
    CROSS JOIN
      UNNEST(arr) AS item
  )
);

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
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  mozfun.glam.histogram_fill_buckets_dirichlet(
    mozfun.map.sum(ARRAY_AGG(record)),
    mozfun.glam.histogram_buckets_cast_string_array(
      udf_get_buckets(metric_type, range_min, range_max, bucket_count)
    ),
    CAST(ROUND(SUM(record.value)) AS INT64)
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix_glam_nightly__histogram_bucket_counts_v1
GROUP BY
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
