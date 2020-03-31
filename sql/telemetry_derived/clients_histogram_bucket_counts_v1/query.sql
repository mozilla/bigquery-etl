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
    old_aggregates ARRAY<STRUCT<key STRING, value INT64>>,
    new_aggregates ARRAY<STRUCT<key STRING, value INT64>>>>)
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
  old_aggregates ARRAY<STRUCT<key STRING, value FLOAT64>>,
  new_aggregates ARRAY<STRUCT<key STRING, value FLOAT64>>>> AS (
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
        udf_normalized_sum(old_aggregates) AS old_aggregates,
        udf_normalized_sum(new_aggregates) AS new_aggregates
      FROM UNNEST(arrs))

    SELECT ARRAY_AGG((
      first_bucket,
      last_bucket,
      num_buckets,
      latest_version,
      metric,
      metric_type,
      key,
      process,
      agg_type,
      old_aggregates,
      new_aggregates
    ))
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
  FROM clients_histogram_aggregates_v1
  WHERE submission_date = @submission_date
    AND updated_on_last_run),

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
    'old' AS recency,
    histogram_aggregates.key AS key,
    aggregates.key AS bucket,
    aggregates.value
  FROM normalized_histograms
  CROSS JOIN UNNEST(histogram_aggregates) AS histogram_aggregates
  CROSS JOIN UNNEST(old_aggregates) AS aggregates

  UNION ALL

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
    'new' AS recency,
    histogram_aggregates.key AS key,
    aggregates.key AS bucket,
    aggregates.value
  FROM normalized_histograms
  CROSS JOIN UNNEST(histogram_aggregates) AS histogram_aggregates
  CROSS JOIN UNNEST(new_aggregates) AS aggregates),

buckets_to_update_summed AS (
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
    recency,
    CAST(bucket AS STRING) AS bucket,
    CASE
      WHEN recency = 'old' THEN 1.0 * SUM(value)
      ELSE NULL
    END AS old_value,
    CASE
      WHEN recency = 'new' THEN 1.0 * SUM(value)
      ELSE NULL
    END AS new_value
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
    recency,
    bucket),

-- Using `MAX` to get the correct/non-null value while grouping rows
buckets_to_update AS (
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
    bucket,
    MAX(old_value) AS old_value,
    MAX(new_value) AS new_value
  FROM buckets_to_update_summed
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
    bucket),

current_buckets AS (
  SELECT *
    EXCEPT(record),
    record.key AS bucket,
    record.value AS value
  FROM clients_histogram_bucket_counts_v1
  WHERE submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
)

SELECT
  @submission_date AS submission_date,
  os,
  app_version,
  app_build_id,
  channel,
  COALESCE(current_buckets.first_bucket, buckets_to_update.first_bucket) AS first_bucket,
  COALESCE(current_buckets.last_bucket, buckets_to_update.last_bucket) AS last_bucket,
  COALESCE(current_buckets.num_buckets, buckets_to_update.num_buckets) AS num_buckets,
  metric,
  COALESCE(current_buckets.metric_type, buckets_to_update.metric_type) AS metric_type,
  key,
  process,
  agg_type,
  STRUCT<key STRING, value FLOAT64>(
    bucket,
    CASE
      WHEN buckets_to_update.new_value IS NULL
      THEN current_buckets.value
      ELSE current_buckets.value - buckets_to_update.old_value + buckets_to_update.new_value
    END
  ) AS record
FROM current_buckets
FULL OUTER JOIN buckets_to_update
USING (os, app_version, app_build_id, channel, metric, key, process, agg_type, bucket)
