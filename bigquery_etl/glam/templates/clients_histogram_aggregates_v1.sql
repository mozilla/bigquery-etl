-- old
WITH clients_histogram_aggregates_partition AS
  (SELECT *
  FROM clients_histogram_aggregates_v1
  WHERE sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id)

SELECT
  sample_id,
  client_id,
  os,
  app_version,
  app_build_id,
  hist_aggs.channel AS channel,
  CONCAT(client_id, os, app_version, app_build_id, hist_aggs.channel) AS join_key,
  histogram_aggregates
FROM clients_histogram_aggregates_partition AS hist_aggs
LEFT JOIN latest_versions
ON latest_versions.channel = hist_aggs.channel
WHERE app_version >= (latest_version - 2)

-- new
WITH filtered_date_channel AS (
  SELECT *
  FROM clients_daily_histogram_aggregates_v1
  WHERE submission_date = @submission_date),

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
  FROM filtered_date_channel
  CROSS JOIN
    UNNEST(histogram_aggregates)
  WHERE value IS NOT NULL
    AND ARRAY_LENGTH(value) > 0
    AND bucket_range.num_buckets > 0
),

version_filtered_new AS
  (SELECT
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
  FROM filtered_aggregates AS hist_aggs
  LEFT JOIN latest_versions
  ON latest_versions.channel = hist_aggs.channel
  WHERE CAST(app_version AS INT64) >= (latest_version - 2)),

aggregated_histograms AS
  (SELECT
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
      latest_version)

SELECT
  udf_js.sample_id(client_id) AS sample_id,
  client_id,
  os,
  app_version,
  app_build_id,
  channel,
  CONCAT(client_id, os, app_version, app_build_id, channel) AS join_key,
  ARRAY_AGG(STRUCT<
    first_bucket INT64,
    last_bucket INT64,
    num_buckets INT64,
    latest_version INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    aggregates ARRAY<STRUCT<key STRING, value INT64>>>(
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
)) AS histogram_aggregates
FROM aggregated_histograms
GROUP BY
  client_id,
  os,
  app_version,
  app_build_id,
  channel

-- merged
WITH old_data AS
  (SELECT *
  FROM clients_histogram_aggregates_old_v1
  WHERE sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id),

new_data AS
  (SELECT *
  FROM clients_histogram_aggregates_new_v1
  WHERE sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id)

SELECT
  COALESCE(old_data.sample_id, new_data.sample_id) AS sample_id,
  COALESCE(old_data.client_id, new_data.client_id) AS client_id,
  COALESCE(old_data.os, new_data.os) AS os,
  COALESCE(old_data.app_version, CAST(new_data.app_version AS INT64)) AS app_version,
  COALESCE(old_data.app_build_id, new_data.app_build_id) AS app_build_id,
  COALESCE(old_data.channel, new_data.channel) AS channel,
  old_data.histogram_aggregates AS old_aggs,
  new_data.histogram_aggregates AS new_aggs
FROM old_data
FULL OUTER JOIN new_data
  ON new_data.join_key = old_data.join_key

-- result
CREATE TEMP FUNCTION udf_merged_user_data(old_aggs ANY TYPE, new_aggs ANY TYPE)
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
    aggregates ARRAY<STRUCT<key STRING, value INT64>>>> AS (
  (
    WITH unnested AS
      (SELECT *
      FROM UNNEST(old_aggs)

      UNION ALL

      SELECT *
      FROM UNNEST(new_aggs)),

    aggregated_data AS
      (SELECT AS STRUCT
        first_bucket,
        last_bucket,
        num_buckets,
        latest_version,
        metric,
        metric_type,
        key,
        process,
        agg_type,
        udf.map_sum(ARRAY_CONCAT_AGG(aggregates)) AS histogram_aggregates
      FROM unnested
      GROUP BY
        first_bucket,
        last_bucket,
        num_buckets,
        latest_version,
        metric,
        metric_type,
        key,
        process,
        agg_type)

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
        histogram_aggregates))
      FROM aggregated_data
  )
);

WITH clients_histogram_merged_partition AS
  (SELECT *
  FROM clients_histogram_aggregates_merged_v1
  WHERE sample_id >= @min_sample_id
    AND sample_id <= @max_sample_id)

SELECT
  sample_id,
  client_id,
  os,
  app_version,
  app_build_id,
  channel,
  udf_merged_user_data(old_aggs, new_aggs) AS histogram_aggregates
FROM clients_histogram_merged_partition
