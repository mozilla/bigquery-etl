-- query for org_mozilla_fenix_glam_nightly__scalar_probe_counts_v1;
SELECT
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  SUM(count) AS total_users,
  mozfun.glam.histogram_fill_buckets_dirichlet(
    mozfun.map.sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
    CASE
    WHEN
      metric_type IN ("counter", "quantity", "labeled_counter", "timespan")
    THEN
      ARRAY(
        SELECT
          FORMAT("%.*f", 2, bucket)
        FROM
          UNNEST(
            mozfun.glam.histogram_generate_scalar_buckets(range_min, range_max, bucket_count)
          ) AS bucket
        ORDER BY
          bucket
      )
    WHEN
      metric_type IN ("boolean")
    THEN
      ['always', 'never', 'sometimes']
    END
    ,
    SUM(count)
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix_glam_nightly__scalar_bucket_counts_v1
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
