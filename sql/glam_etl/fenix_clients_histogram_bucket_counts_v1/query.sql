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
    os,
    app_version,
    app_build_id,
    channel,
    latest_version,
    metric,
    metric_type,
    key,
    agg_type,
    histogram_aggregates.key AS key,
    aggregates.key AS bucket,
    aggregates.value
  FROM
    normalized_histograms
  CROSS JOIN
    UNNEST(histogram_aggregates) AS histogram_aggregates
  CROSS JOIN
    UNNEST(aggregates) AS aggregates
)
SELECT
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
