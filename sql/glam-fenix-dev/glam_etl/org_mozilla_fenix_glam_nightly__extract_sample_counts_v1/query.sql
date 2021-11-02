-- query for org_mozilla_fenix_glam_nightly__extract_sample_counts_v1;
SELECT
  channel,
  app_version,
  metric,
  key,
  coalesce(ping_type, "*") AS ping_type,
  COALESCE(app_build_id, "*") AS app_build_id,
  IF(
    app_build_id = "*",
    NULL,
    SAFE_CAST(mozfun.glam.build_hour_to_datetime(app_build_id) AS STRING)
  ) AS build_date,
  COALESCE(os, "*") AS os,
  total_sample
FROM
  `glam_etl.org_mozilla_fenix_glam_nightly__view_sample_counts_v1`
