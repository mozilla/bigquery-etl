SELECT
  CASE
  WHEN
    channel = "nightly"
  THEN
    1
  WHEN
    channel = "beta"
  THEN
    2
  WHEN
    channel = "release"
  THEN
    3
  END
  AS channel,
  app_version,
  COALESCE(app_build_id, "*") AS app_build_id,
  COALESCE(os, "*") AS os,
  metric,
  key,
  total_sample
FROM
  `moz-fx-data-shared-prod.telemetry_derived.glam_sample_counts_v1`
WHERE
  app_version IS NOT NULL
  AND total_sample > 100
