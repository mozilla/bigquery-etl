WITH flat_clients_scalar_aggregates AS (
  SELECT *,
    os = 'Windows' and channel = 'release' AS sampled,
  FROM
    clients_scalar_aggregates_v1
  WHERE
    submission_date = @submission_date
    AND (
      @app_version IS NULL
      OR app_version = @app_version
    )
),

static_combos as (
  SELECT null as os, null as app_build_id
  UNION ALL
  SELECT null as os, '*' as app_build_id
  UNION ALL
  SELECT '*' as os, null as app_build_id
  UNION ALL
  SELECT '*' as os, '*' as app_build_id
),

all_combos AS (
  SELECT
    * EXCEPT(os, app_build_id),
    COALESCE(combos.os, flat_table.os) as os,
    COALESCE(combos.app_build_id, flat_table.app_build_id) as app_build_id
  FROM
     flat_clients_scalar_aggregates flat_table
  CROSS JOIN
     static_combos combos),

user_aggregates AS (
  SELECT
    client_id,
    IF(os = '*', NULL, os) AS os,
    app_version,
    IF(app_build_id = '*', NULL, app_build_id) AS app_build_id,
    channel,
    IF(MAX(sampled), 10, 1) AS user_count,
    udf.merge_scalar_user_data(ARRAY_CONCAT_AGG(scalar_aggregates)) AS scalar_aggregates
  FROM
    all_combos
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel),

percentiles AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    -- empty columns to match clients_histogram_probe_counts_v1 schema
    NULL AS first_bucket,
    NULL AS last_bucket,
    NULL AS num_buckets,
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    SUM(user_count) AS total_users,
    APPROX_QUANTILES(value, 1000)  AS aggregates
  FROM
    user_aggregates
  CROSS JOIN UNNEST(scalar_aggregates)
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type
),
aggregated AS (
  SELECT *
  REPLACE(mozfun.glam.map_from_array_offsets_precise(
    [0.1, 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0, 99.9],
    aggregates
  ) AS aggregates)
  FROM percentiles
)
SELECT
  *,
  aggregates AS non_norm_aggregates
FROM
  aggregated
