CREATE TEMP FUNCTION udf_normalized_sum(arrs ARRAY<STRUCT<key STRING, value INT64>>)
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

CREATE TEMP FUNCTION udf_normalize_histograms(
  arrs ARRAY<
    STRUCT<
      latest_version INT64,
      metric STRING,
      metric_type STRING,
      key STRING,
      agg_type STRING,
      sum INT64,
      value ARRAY<STRUCT<key STRING, value INT64>>
    >
  >
)
RETURNS ARRAY<
  STRUCT<
    latest_version INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    agg_type STRING,
    aggregates ARRAY<STRUCT<key STRING, value FLOAT64>>
  >
> AS (
  (
    WITH normalized AS (
      SELECT
        latest_version,
        metric,
        metric_type,
        key,
        agg_type,
        -- NOTE: dropping the actual sum here, since it isn't being used
        udf_normalized_sum(value) AS aggregates
      FROM
        UNNEST(arrs)
    )
    SELECT
      ARRAY_AGG((latest_version, metric, metric_type, key, agg_type, aggregates))
    FROM
      normalized
  )
);

WITH normalized_histograms AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    udf_normalize_histograms(histogram_aggregates) AS histogram_aggregates
  FROM
    glam_etl.fenix_clients_histogram_aggregates_v1
),
unnested AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    histogram_aggregates.latest_version AS latest_version,
    histogram_aggregates.metric AS metric,
    histogram_aggregates.metric_type AS metric_type,
    histogram_aggregates.key AS key,
    histogram_aggregates.agg_type AS agg_type,
    aggregates.key AS bucket,
    aggregates.value
  FROM
    normalized_histograms,
    UNNEST(histogram_aggregates) AS histogram_aggregates,
    UNNEST(aggregates) AS aggregates
),
-- Find information that can be used to construct the bucket range.
-- TODO: is there a better way to incorporate the bucket information and to
-- validate the correctness of these buckets. The Glean documentation does
-- mention that there is a maximum recorded size, and that there are 8 buckets
-- for each power of 2. This doesn't apply to the custom distributions
-- (GeckoView), which would need to incorporate information from the probe info
-- service.
distribution_metadata AS (
  SELECT
    metric,
    MIN(CAST(bucket AS INT64)) AS first_bucket,
    MAX(CAST(bucket AS INT64)) AS last_bucket,
    COUNT(DISTINCT bucket) AS num_buckets
  FROM
    unnested
  GROUP BY
    metric
),
records AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    latest_version,
    metric,
    metric_type,
    key,
    agg_type,
    STRUCT<key STRING, value FLOAT64>(CAST(bucket AS STRING), 1.0 * SUM(value)) AS record
  FROM
    unnested
  GROUP BY
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    latest_version,
    metric,
    metric_type,
    key,
    agg_type,
    bucket
)
SELECT
  *
FROM
  records
JOIN
  distribution_metadata
USING
  (metric)
