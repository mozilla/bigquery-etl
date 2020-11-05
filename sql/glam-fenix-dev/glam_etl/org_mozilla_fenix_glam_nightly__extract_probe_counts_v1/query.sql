-- query for org_mozilla_fenix_glam_nightly__extract_probe_counts_v1;
SELECT
  channel,
  app_version AS version,
  ping_type,
  os,
  app_build_id AS build_id,
  IF(
    app_build_id = "*",
    NULL,
    SAFE_CAST(mozfun.glam.build_hour_to_datetime(app_build_id) AS STRING)
  ) AS build_date,
  metric,
  metric_type,
    -- BigQuery has some null unicode characters which Postgresql doesn't like,
    -- so we remove those here. Also limit string length to 200 to match column
    -- length.
  SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS metric_key,
  client_agg_type,
  MAX(total_users) AS total_users,
  MAX(IF(agg_type = "histogram", mozfun.glam.histogram_cast_json(aggregates), NULL)) AS histogram,
  MAX(
    IF(agg_type = "percentiles", mozfun.glam.histogram_cast_json(aggregates), NULL)
  ) AS percentiles,
FROM
  `glam_etl.org_mozilla_fenix_glam_nightly__view_probe_counts_v1`
WHERE
  total_users > 10
GROUP BY
  channel,
  app_version,
  ping_type,
  os,
  app_build_id,
  metric,
  metric_type,
  key,
  client_agg_type
