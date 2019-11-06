CREATE TEMP FUNCTION udf_map_sum(entries ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      key,
      SUM(value) AS value
    FROM
      UNNEST(entries)
    GROUP BY
      key
  )
);
--
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
        STRUCT<key STRING, value FLOAT64>(k, 1.0 * v / total_count) AS record
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

WITH latest_versions AS (
  SELECT channel, MAX(CAST(app_version AS INT64)) AS latest_version
  FROM
    (SELECT
      normalized_channel AS channel,
      SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
      COUNT(*)
    FROM `moz-fx-data-shared-prod.telemetry_stable.main_v4`
    WHERE DATE(submission_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      AND normalized_channel IN ("nightly", "beta", "release")
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT client_id) > 2000
    ORDER BY 1, 2 DESC)
  GROUP BY 1),

filtered_date_channel AS (
  SELECT *
  FROM clients_daily_histogram_aggregates_v1
  WHERE submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND channel IN ('release', 'beta', 'nightly')
),

filtered_aggregates AS (
  SELECT
    submission_date,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    bucket_range,
    metric,
    metric_type,
    key,
    agg_type,
    value
  FROM filtered_date_channel
  CROSS JOIN
    UNNEST(histogram_aggregates)
  WHERE value IS NOT NULL
),

version_filtered AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      hist_aggs.channel AS channel,
      bucket_range.first_bucket AS first_bucket,
      bucket_range.last_bucket AS last_bucket,
      bucket_range.num_buckets AS num_buckets,
      metric,
      metric_type,
      key,
      agg_type,
      latest_version,
      hist_aggs.value AS value
  FROM filtered_aggregates AS hist_aggs
  LEFT JOIN latest_versions
  ON latest_versions.channel = hist_aggs.channel
  WHERE CAST(app_version AS INT64) >= (latest_version - 2)),

normalized_histograms AS
  (SELECT
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
      agg_type,
      latest_version,
      udf_normalized_sum(udf_map_sum(ARRAY_CONCAT_AGG(value))) AS aggregates
  FROM
      version_filtered
  GROUP BY
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
      agg_type,
      latest_version)

SELECT
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
    normalized_histograms.key AS key,
    agg_type,
    agg.key AS bucket,
    agg.value AS value
FROM normalized_histograms
CROSS JOIN UNNEST(aggregates) AS agg
WHERE num_buckets > 0
