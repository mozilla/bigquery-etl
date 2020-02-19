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
