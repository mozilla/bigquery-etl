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
    clients_histogram_aggregates_v2
  CROSS JOIN
    UNNEST(histogram_aggregates)
  WHERE
    first_bucket IS NOT NULL
    AND sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id
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
    -- Filter out builds having less than 0.5% of WAU, considering sampling
    -- for context see https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
    CASE
      WHEN channel = 'release'
        THEN COUNT(DISTINCT client_id) > 625000 / (@max_sample_id - @min_sample_id + 1)
      WHEN channel = 'beta'
        THEN COUNT(DISTINCT client_id) > 9000 / (@max_sample_id - @min_sample_id + 1)
      WHEN channel = 'nightly'
        THEN COUNT(DISTINCT client_id) > 375 / (@max_sample_id - @min_sample_id + 1)
      ELSE COUNT(DISTINCT client_id) > 100 / (@max_sample_id - @min_sample_id + 1)
    END
),
data_with_enough_wau AS (
  SELECT
    *
  FROM
    filtered_data table
  INNER JOIN
    build_ids
    USING (app_build_id, channel)
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
      ) AS aggregates,
      COALESCE(CAST(NULL AS STRING), os) AS os,
      COALESCE(CAST(NULL AS STRING), app_build_id) AS app_build_id
    )
  FROM
    data_with_enough_wau
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
  UNION ALL
  SELECT
    * EXCEPT (sampled) REPLACE(
    -- This returns true if at least 1 row has sampled=true.
    -- ~0.0025% of the population uses more than 1 os for the same set of dimensions
    -- and in this case we treat them as Windows+Release users when fudging numbers
      mozfun.glam.histogram_normalized_sum(
        mozfun.map.sum(ARRAY_CONCAT_AGG(aggregates)),
        IF(MAX(sampled), 10.0, 1.0)
      ) AS aggregates,
      "*" AS os,
      COALESCE(CAST(NULL AS STRING), app_build_id) AS app_build_id
    )
  FROM
    data_with_enough_wau
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
  UNION ALL
  SELECT
    * EXCEPT (sampled) REPLACE(
    -- This returns true if at least 1 row has sampled=true.
    -- ~0.0025% of the population uses more than 1 os for the same set of dimensions
    -- and in this case we treat them as Windows+Release users when fudging numbers
      mozfun.glam.histogram_normalized_sum(
        mozfun.map.sum(ARRAY_CONCAT_AGG(aggregates)),
        IF(MAX(sampled), 10.0, 1.0)
      ) AS aggregates,
      COALESCE(CAST(NULL AS STRING), os) AS os,
      "*" AS app_build_id
    )
  FROM
    data_with_enough_wau
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
  UNION ALL
  SELECT
    * EXCEPT (sampled) REPLACE(
    -- This returns true if at least 1 row has sampled=true.
    -- ~0.0025% of the population uses more than 1 os for the same set of dimensions
    -- and in this case we treat them as Windows+Release users when fudging numbers
      mozfun.glam.histogram_normalized_sum(
        mozfun.map.sum(ARRAY_CONCAT_AGG(aggregates)),
        IF(MAX(sampled), 10.0, 1.0)
      ) AS aggregates,
      "*" AS os,
      "*" AS app_build_id
    )
  FROM
    data_with_enough_wau
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
