WITH filtered_data AS (
  SELECT
    sample_id,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    aggregates,
    os = 'Windows'
    AND channel = 'release' AS sampled
  FROM
    clients_histogram_aggregates_v1
  CROSS JOIN
    UNNEST(histogram_aggregates)
  WHERE
    submission_date = @submission_date
    AND first_bucket IS NOT NULL
    AND sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id
),
static_combos AS (
  SELECT
    NULL AS os,
    NULL AS app_build_id
  UNION ALL
  SELECT
    NULL AS os,
    '*' AS app_build_id
  UNION ALL
  SELECT
    '*' AS os,
    NULL AS app_build_id
  UNION ALL
  SELECT
    '*' AS os,
    '*' AS app_build_id
),
all_combos AS (
  SELECT
    * EXCEPT (os, app_build_id),
    COALESCE(combo.os, table.os) AS os,
    COALESCE(combo.app_build_id, table.app_build_id) AS app_build_id
  FROM
    filtered_data table
  CROSS JOIN
    static_combos combo
),
normalized_histograms AS (
  SELECT
    * EXCEPT (sampled) REPLACE(
    -- This returns true if at least 1 row has sampled=true.
    -- ~0.0025% of the population uses more than 1 os for the same set of dimensions
    -- and in this case we treat them as Windows+Release users when fudging numbers
      mozfun.glam.histogram_normalized_sum(
        mozfun.map.sum(ARRAY_CONCAT_AGG(aggregates)),
        IF(MAX(sampled), 10.0, 1.0)
      ) AS aggregates
    )
  FROM
    all_combos
  GROUP BY
    sample_id,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    process,
    agg_type
)
SELECT
  os,
  app_version,
  app_build_id,
  channel,
  first_bucket,
  last_bucket,
  num_buckets,
  metric,
  metric_type,
  normalized_histograms.key AS key,
  process,
  agg_type,
  STRUCT<key STRING, value FLOAT64>(
    CAST(aggregates.key AS STRING),
    1.0 * SUM(aggregates.value)
  ) AS record
FROM
  normalized_histograms
CROSS JOIN
  UNNEST(aggregates) AS aggregates
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  first_bucket,
  last_bucket,
  num_buckets,
  metric,
  metric_type,
  key,
  process,
  agg_type,
  aggregates.key
