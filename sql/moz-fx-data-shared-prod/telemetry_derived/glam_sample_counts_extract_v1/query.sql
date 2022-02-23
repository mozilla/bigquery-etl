SELECT
  CASE
  WHEN
    cp.channel = "nightly"
  THEN
    1
  WHEN
    cp.channel = "beta"
  THEN
    2
  WHEN
    cp.channel = "release"
  THEN
    3
  END
  AS channel,
  cp.app_version,
  COALESCE(cp.app_build_id, "*") AS app_build_id,
  COALESCE(cp.os, "*") AS os,
  process,
  cp.metric,
  SUBSTR(REPLACE(cp.key, r"\x00", ""), 0, 200) AS key,
  client_agg_type,
  total_sample,
FROM
  `moz-fx-data-shared-prod.telemetry.client_probe_counts` cp
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry_derived.glam_sample_counts_v1` sc
ON
  sc.os = COALESCE(cp.os, "*")
  AND sc.app_build_id = COALESCE(cp.app_build_id, "*")
  AND sc.app_version = cp.app_version
  AND sc.metric = cp.metric
  AND sc.key = cp.key
  AND total_sample IS NOT NULL
WHERE
  cp.app_version IS NOT NULL
  AND cp.total_users > 375
  AND client_agg_type NOT IN ('sum', 'min', 'avg', 'max')
