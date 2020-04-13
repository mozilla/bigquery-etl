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
    latest_version,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    aggregates
  FROM
    clients_histogram_aggregates_v1
  CROSS JOIN UNNEST(histogram_aggregates)
  WHERE submission_date = @submission_date
    AND first_bucket IS NOT NULL),

aggregated_histograms AS
  (SELECT
    sample_id,
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
    key,
    process,
    agg_type,
    aggregates
  FROM filtered_data
  WHERE os IS NOT NULL

  UNION ALL

  SELECT
    sample_id,
    client_id,
    NULL AS os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    latest_version,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    udf.map_sum(ARRAY_CONCAT_AGG(aggregates)) AS aggregates
  FROM filtered_data
  GROUP BY
    sample_id,
    client_id,
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
    latest_version

  UNION ALL

  SELECT
    sample_id,
    client_id,
    os,
    app_version,
    NULL AS app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    latest_version,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    udf.map_sum(ARRAY_CONCAT_AGG(aggregates)) AS aggregates
  FROM filtered_data
  WHERE os IS NOT NULL
  GROUP BY
    sample_id,
    client_id,
    os,
    app_version,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    latest_version

  UNION ALL

  SELECT
    sample_id,
    client_id,
    NULL AS os,
    app_version,
    NULL AS app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    latest_version,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    udf.map_sum(ARRAY_CONCAT_AGG(aggregates)) AS aggregates
  FROM filtered_data
  GROUP BY
    sample_id,
    client_id,
    app_version,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    latest_version),

normalized_histograms AS (
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
    latest_version,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    udf_normalized_sum(aggregates) AS aggregates
  FROM aggregated_histograms)

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
    1.0 * SUM(value)
  ) AS record
FROM normalized_histograms
CROSS JOIN UNNEST(aggregates) AS aggregates
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
