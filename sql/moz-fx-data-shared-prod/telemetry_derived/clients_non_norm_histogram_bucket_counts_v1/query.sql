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
    `moz-fx-data-shared-prod.telemetry_derived.clients_histogram_aggregates_v2`
  CROSS JOIN
    UNNEST(histogram_aggregates)
  WHERE
    first_bucket IS NOT NULL
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
build_ids AS (
  SELECT
    app_build_id,
    channel,
  FROM
    filtered_data
  GROUP BY
    1,
    2
  HAVING
    -- Filter out builds having less than 0.5% of WAU
    -- for context see https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
    CASE
      WHEN channel = 'release'
        THEN COUNT(DISTINCT client_id) > 625000/(@max_sample_id - @min_sample_id + 1)
      WHEN channel = 'beta'
        THEN COUNT(DISTINCT client_id) > 9000/(@max_sample_id - @min_sample_id + 1)
      WHEN channel = 'nightly'
        THEN COUNT(DISTINCT client_id) > 375/(@max_sample_id - @min_sample_id + 1)
      ELSE COUNT(DISTINCT client_id) > 100/(@max_sample_id - @min_sample_id + 1)
    END
),
all_combos as (
  SELECT
    * EXCEPT (os, app_build_id),
    COALESCE(combo.os, table.os) AS os,
    COALESCE(combo.app_build_id, table.app_build_id) AS app_build_id
  FROM
    filtered_data table
  INNER JOIN
    build_ids
    USING (app_build_id, channel)
  CROSS JOIN
    static_combos combo
),
non_normalized_histograms AS (
  SELECT
    * EXCEPT (sampled) REPLACE(
        mozfun.map.sum(ARRAY_CONCAT_AGG(aggregates)) AS aggregates
      )
  FROM
    all_combos
  WHERE sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id
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
  non_normalized_histograms.key AS key,
  process,
  agg_type,
  STRUCT<key STRING, value FLOAT64>(
    CAST(aggregates.key AS STRING),
    1.0 * SUM(aggregates.value)
  ) AS record
FROM
  non_normalized_histograms
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
