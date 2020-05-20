CREATE TEMP FUNCTION udf_normalized_sum (arrs ARRAY<STRUCT<key STRING, value INT64>>, sampled BOOL)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Input: one histogram for a single client.
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
          -- Weight probes from Windows release clients to account for 10% sampling
          COALESCE(SAFE_DIVIDE(1.0 * v, total_count), 0) * IF(sampled, 10, 1)
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
    metric,
    metric_type,
    key,
    process,
    agg_type,
    aggregates,
    os = 'Windows'and channel = 'release' AS sampled
  FROM
    clients_histogram_aggregates_v1
  CROSS JOIN UNNEST(histogram_aggregates)
  WHERE submission_date = @submission_date
    AND first_bucket IS NOT NULL
    AND sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id),

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
    * except(os, app_build_id),
    COALESCE(combo.os, table.os) as os,
    COALESCE(combo.app_build_id, table.app_build_id) as app_build_id
  FROM
     filtered_data table
  CROSS JOIN
     static_combos combo),

normalized_histograms AS
  (SELECT * EXCEPT(sampled) REPLACE(
    -- This returns true if at least 1 row has sampled=true.
    -- ~0.0025% of the population uses more than 1 os for the same set of dimensions
    -- and in this case we treat them as Windows+Release users when fudging numbers
    udf_normalized_sum(udf.map_sum(ARRAY_CONCAT_AGG(aggregates)), MAX(sampled)) AS aggregates)
  FROM all_combos
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
    agg_type)

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
