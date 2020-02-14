CREATE TEMP FUNCTION udf_normalized_sum (arrs ARRAY<STRUCT<key STRING, value INT64>>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Returns the normalized sum of the input maps.
  -- It returns the total_count[k] / SUM(total_count)
  -- for each key k.
  (
    WITH total_counts AS (
      SELECT
        sum(a.value) AS total_count
      FROM
        UNNEST(arrs) AS a
    ),

    summed_counts AS (
      SELECT
        a.key AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(arrs) AS a
      GROUP BY
        a.key
    ),

    final_values AS (
      SELECT
        STRUCT<key STRING, value FLOAT64>(
          k,
          COALESCE(SAFE_DIVIDE(1.0 * v, total_count), 0)
        ) AS record
      FROM
        summed_counts
      CROSS JOIN
        total_counts
    )

    SELECT
        ARRAY_AGG(record)
    FROM
      final_values
  )
);

CREATE TEMP FUNCTION udf_normalize_histograms (
  arrs ARRAY<STRUCT<
    first_bucket INT64,
    last_bucket INT64,
    num_buckets INT64,
    latest_version INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    aggregates ARRAY<STRUCT<key STRING, value INT64>>>>)
RETURNS ARRAY<STRUCT<
  first_bucket INT64,
  last_bucket INT64,
  num_buckets INT64,
  latest_version INT64,
  metric STRING,
  metric_type STRING,
  key STRING,
  process STRING,
  agg_type STRING,
  aggregates ARRAY<STRUCT<key STRING, value FLOAT64>>>> AS (
(
    WITH normalized AS (
      SELECT
        first_bucket,
        last_bucket,
        num_buckets,
        latest_version,
        metric,
        metric_type,
        key,
        process,
        agg_type,
        udf_normalized_sum(aggregates) AS aggregates
      FROM UNNEST(arrs))

    SELECT ARRAY_AGG((first_bucket, last_bucket, num_buckets, latest_version, metric, metric_type, key, process, agg_type, aggregates))
    FROM normalized
));

WITH normalized_histograms AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    udf_normalize_histograms(histogram_aggregates) AS histogram_aggregates
  FROM clients_histogram_aggregates_v1),

unnested AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    latest_version,
    metric,
    metric_type,
    process,
    agg_type,
    histogram_aggregates.key AS key,
    aggregates.key AS bucket,
    aggregates.value
  FROM normalized_histograms
  CROSS JOIN UNNEST(histogram_aggregates) AS histogram_aggregates
  CROSS JOIN UNNEST(aggregates) AS aggregates)

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
  key,
  process,
  agg_type,
  STRUCT<key STRING, value FLOAT64>(
    CAST(bucket AS STRING),
    1.0 * SUM(value)
  ) AS record
FROM unnested
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
  bucket
