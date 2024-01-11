WITH filtered_date_channel AS (
  SELECT
    *
  FROM
    clients_daily_histogram_aggregates_v1
  WHERE
    submission_date = @submission_date
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
    process,
    agg_type,
    value
  FROM
    filtered_date_channel
  CROSS JOIN
    UNNEST(histogram_aggregates)
  WHERE
    value IS NOT NULL
    AND ARRAY_LENGTH(value) > 0
    AND bucket_range.num_buckets > 0
),
version_filtered_new AS (
  SELECT
    submission_date,
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
    process,
    agg_type,
    latest_version,
    hist_aggs.value AS value
  FROM
    filtered_aggregates AS hist_aggs
  LEFT JOIN
    latest_versions
    ON latest_versions.channel = hist_aggs.channel
  WHERE
    CAST(app_version AS INT64) >= (latest_version - 2)
),
aggregated_histograms AS (
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
    key,
    process,
    agg_type,
    udf.map_sum(ARRAY_CONCAT_AGG(value)) AS aggregates
  FROM
    version_filtered_new
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
    process,
    agg_type,
    latest_version
)
SELECT
  udf_js.sample_id(client_id) AS sample_id,
  client_id,
  os,
  app_version,
  app_build_id,
  channel,
  CONCAT(client_id, os, app_version, app_build_id, channel) AS join_key,
  ARRAY_AGG(
    STRUCT<
      first_bucket INT64,
      last_bucket INT64,
      num_buckets INT64,
      latest_version INT64,
      metric STRING,
      metric_type STRING,
      key STRING,
      process STRING,
      agg_type STRING,
      aggregates ARRAY<STRUCT<key STRING, value INT64>>
    >(
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
    )
  ) AS histogram_aggregates
FROM
  aggregated_histograms
GROUP BY
  client_id,
  os,
  app_version,
  app_build_id,
  channel
