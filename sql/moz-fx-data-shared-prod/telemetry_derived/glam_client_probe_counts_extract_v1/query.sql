WITH final_probe_extract AS (
  SELECT
    app_version,
    COALESCE(os, "*") AS os,
    COALESCE(app_build_id, "*") AS app_build_id,
    process,
    metric,
    SUBSTR(REPLACE(KEY, r"\x00", ""), 0, 200) AS KEY,
    client_agg_type,
    metric_type,
    total_users,
      -- Using MAX instead of COALESCE since this is not in the GROUP BY.
    MAX(IF(agg_type = "histogram", mozfun.glam.histogram_cast_json(aggregates), NULL)) AS histogram,
    MAX(
      IF(agg_type = "histogram", mozfun.glam.histogram_cast_json(non_norm_aggregates), NULL)
    ) AS non_norm_histogram,
    MAX(
      IF(agg_type = "percentiles", mozfun.glam.histogram_cast_json(aggregates), NULL)
    ) AS percentiles,
    MAX(
      IF(agg_type = "percentiles", mozfun.glam.histogram_cast_json(non_norm_aggregates), NULL)
    ) AS non_norm_percentiles
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.client_probe_counts`
  WHERE
    channel = @channel
    AND app_version IS NOT NULL
    AND total_users > 375
  GROUP BY
    channel,
    app_version,
    app_build_id,
    os,
    metric,
    metric_type,
    KEY,
    process,
    client_agg_type,
    total_users
),
glam_sample_counts AS (
  SELECT
    fsc1.os,
    fsc1.app_version,
    fsc1.app_build_id,
    fsc1.metric,
    fsc1.key,
    fsc1.process,
    fsc1.agg_type,
    CASE
      WHEN fsc1.agg_type IN ('max', 'min', 'sum', 'avg')
        AND fsc2.agg_type = 'count'
        THEN fsc2.total_sample
      ELSE fsc1.total_sample
    END AS total_sample
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.glam_sample_counts_v1` fsc1
  INNER JOIN
    `moz-fx-data-shared-prod.telemetry_derived.glam_sample_counts_v1` fsc2
    ON fsc1.os = fsc2.os
    AND fsc1.app_build_id = fsc2.app_build_id
    AND fsc1.app_version = fsc2.app_version
    AND fsc1.metric = fsc2.metric
    AND fsc1.key = fsc2.key
    AND fsc1.channel = fsc2.channel
    AND fsc1.process = fsc2.process
  WHERE
    fsc1.channel = @channel
)
SELECT
  cp.app_version,
  cp.os,
  cp.app_build_id,
  cp.process,
  cp.metric,
  cp.key,
  cp.client_agg_type,
  cp.metric_type,
  total_users,
  histogram,
  percentiles,
  CASE
    WHEN client_agg_type = ''
      THEN 0
    ELSE total_sample
  END AS total_sample,
  non_norm_histogram,
  non_norm_percentiles
FROM
  final_probe_extract cp
LEFT JOIN
  glam_sample_counts sc
  ON sc.os = cp.os
  AND sc.app_build_id = cp.app_build_id
  AND sc.app_version = cp.app_version
  AND sc.metric = cp.metric
  AND sc.key = cp.key
  AND sc.process = cp.process
  AND total_sample IS NOT NULL
  AND (sc.agg_type = cp.client_agg_type OR cp.client_agg_type = '')
GROUP BY
  cp.app_version,
  cp.os,
  cp.app_build_id,
  cp.process,
  cp.metric,
  cp.key,
  cp.client_agg_type,
  cp.metric_type,
  total_users,
  total_sample,
  histogram,
  non_norm_histogram,
  percentiles,
  non_norm_percentiles
