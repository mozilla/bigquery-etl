WITH sample_counts_filtered AS (
  SELECT
    cp.channel,
    cp.app_version,
    cp.ping_type,
    COALESCE(cp.app_build_id, "*") AS app_build_id,
    COALESCE(cp.os, "*") AS os,
    cp.metric,
    SUBSTR(REPLACE(cp.key, r"\x00", ""), 0, 200) AS key,
    client_agg_type,
    total_sample,
  FROM
  `{{ dataset }}.{{ prefix }}__view_probe_counts_v1` cp
INNER JOIN  `{{ dataset }}.{{ prefix }}__view_sample_counts_v1` sc
  ON
    sc.os = COALESCE(cp.os, "*")
    AND sc.app_build_id = COALESCE(cp.app_build_id, "*")
    AND sc.app_version = cp.app_version
    AND sc.metric = cp.metric
    AND sc.key = cp.key
    AND sc.ping_type = cp.ping_type
    AND total_sample IS NOT NULL
  WHERE
    cp.app_version IS NOT NULL
    AND cp.total_users > {{ total_users }}
    AND client_agg_type NOT IN ('sum', 'min', 'avg', 'max')
),
sample_counts_ranked AS (
  SELECT
    channel,
    app_version,
    ping_type,
    app_build_id,
    os,
    metric,
    key,
    client_agg_type,
    total_sample,
    ROW_NUMBER() OVER (
      PARTITION BY
        channel,
        app_version,
        app_build_id,
        os,
        metric,
        key,
        client_agg_type
      ORDER BY
        total_sample DESC
    ) AS rnk
  FROM
    sample_counts_filtered
)
SELECT
  channel,
  app_version,
  ping_type,
  app_build_id,
  os,
  metric,
  key,
  client_agg_type,
  total_sample
FROM
  sample_counts_ranked
WHERE
  rnk = 1