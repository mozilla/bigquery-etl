SELECT
  cp.channel,
  cp.app_version,
  cp.ping_type,
  COALESCE(cp.app_build_id, "*") AS app_build_id,
  COALESCE(cp.os, "*") AS os,
  cp.metric,
  cp.key,
  cp.client_agg_type,
  total_sample
FROM
  `glam_etl.org_mozilla_fenix_glam_nightly__view_probe_counts_v1` cp
LEFT JOIN
  `glam_etl.org_mozilla_fenix_glam_nightly__view_sample_counts_v1` sc
ON
  sc.os = COALESCE(cp.os, "*")
  AND sc.app_build_id = COALESCE(cp.app_build_id, "*")
  AND sc.app_version = cp.app_version
  AND sc.metric = cp.metric
  AND cp.key = sc.key
  AND total_sample IS NOT NULL
WHERE
  cp.app_version IS NOT NULL
  AND total_users > 10
  AND cp.client_agg_type NOT IN ('sum', 'min', 'avg', 'max')
GROUP BY
  cp.channel,
  cp.ping_type,
  cp.app_version,
  cp.app_build_id,
  cp.os,
  cp.metric,
  cp.key,
  client_agg_type,
  total_sample
