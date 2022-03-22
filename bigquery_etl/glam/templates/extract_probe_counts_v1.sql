{{ header }}

WITH final_probe_extract AS ( SELECT
    channel,
    app_version as version,
    ping_type,
    os,
    app_build_id as build_id,
    IF(app_build_id="*", NULL, SAFE_CAST({{ build_date_udf }}(app_build_id) AS STRING)) as build_date,
    metric,
    metric_type,
    -- BigQuery has some null unicode characters which Postgresql doesn't like,
    -- so we remove those here. Also limit string length to 200 to match column
    -- length.
    SUBSTR(REPLACE(key, r"\x00", ""), 0, 200) AS metric_key,
    client_agg_type,
    MAX(total_users) as total_users,
    MAX(IF(agg_type = "histogram", mozfun.glam.histogram_cast_json(aggregates), NULL)) as histogram,
    MAX(IF(agg_type = "percentiles", mozfun.glam.histogram_cast_json(aggregates), NULL)) as percentiles,
FROM
    `{{ dataset }}.{{ prefix }}__view_probe_counts_v1`
WHERE
    total_users > {{ total_users }}
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
),
-- to populate total_sample for agg_type other than 'count'
glam_sample_counts AS (
  SELECT fsc1.os,
    fsc1.app_version,
    fsc1.app_build_id,
    fsc1.metric,
    fsc1.key,
    fsc1.ping_type,
    fsc1.agg_type,
    CASE WHEN fsc1.agg_type in ('max','min','sum','avg') AND fsc2.agg_type = 'count' THEN fsc2.total_sample ELSE fsc1.total_sample END as total_sample
  FROM `{{ dataset }}.{{ prefix }}__view_sample_counts_v1`  fsc1
  INNER JOIN `{{ dataset }}.{{ prefix }}__view_sample_counts_v1`  fsc2
  ON fsc1.os = fsc2.os
    AND fsc1.app_build_id = fsc2.app_build_id
    AND fsc1.app_version = fsc2.app_version
    AND fsc1.metric = fsc2.metric
    AND fsc1.key = fsc2.key
    AND fsc1.ping_type = fsc2.ping_type
WHERE fsc2.agg_type = 'count'
),
-- get all the rcords from view_probe_counts and the matching from view_sample_counts
ranked_data AS (SELECT
  cp.channel,
  cp.version,
  cp.os,
  cp.ping_type,
  cp.build_id,
  cp.build_date,
  cp.metric,
  cp.metric_key,
  cp.client_agg_type,
  cp.metric_type,
  total_users,
  histogram,
  percentiles,
  CASE WHEN client_agg_type = '' THEN 0 ELSE total_sample END AS total_sample,
  ROW_NUMBER() OVER (PARTITION BY cp.version, cp.os, cp.build_id,cp.ping_type, cp.metric, cp.metric_key, cp.client_agg_type,cp.metric_type,histogram, percentiles
                    ORDER BY total_users, total_sample DESC) as rnk
FROM
  final_probe_extract cp
LEFT JOIN glam_sample_counts sc
  ON
    sc.os = cp.os
    AND sc.app_build_id = cp.build_id
    AND sc.app_version = cp.version
    AND sc.metric = cp.metric
    AND sc.key = cp.metric_key
    AND total_sample IS NOT NULL
    AND (sc.agg_type = cp.client_agg_type OR cp.client_agg_type='')
)
--remove duplicates
SELECT channel,
  version,
  ping_type,
  os,
  build_id,
  build_date,
  metric,
  metric_type,
  metric_key,
  client_agg_type,
  total_users,
  histogram,
  percentiles,
  CAST(total_sample as INT) total_sample
FROM ranked_data
WHERE rnk = 1
