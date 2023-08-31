CREATE TEMP FUNCTION udf_use_old_data(old_aggs ANY TYPE)
RETURNS ARRAY<
  STRUCT<
    first_bucket INT64,
    last_bucket INT64,
    num_buckets INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    aggregates ARRAY<STRUCT<key STRING, value INT64>>
  >
> AS (
  (SELECT old_aggs)
);

CREATE TEMP FUNCTION udf_merged_user_data(old_aggs ANY TYPE, new_aggs ANY TYPE)
RETURNS ARRAY<
  STRUCT<
    first_bucket INT64,
    last_bucket INT64,
    num_buckets INT64,
    metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    aggregates ARRAY<STRUCT<key STRING, value INT64>>
  >
> AS (
  (
    WITH unnested AS (
      SELECT
        *
      FROM
        UNNEST(old_aggs)
      UNION ALL
      SELECT
        *
      FROM
        UNNEST(new_aggs)
    ),
    aggregated_data AS (
      SELECT AS STRUCT
        first_bucket,
        last_bucket,
        num_buckets,
        metric,
        metric_type,
        key,
        process,
        agg_type,
        mozfun.map.sum(ARRAY_CONCAT_AGG(aggregates)) AS histogram_aggregates
      FROM
        unnested
      GROUP BY
        first_bucket,
        last_bucket,
        num_buckets,
        metric,
        metric_type,
        key,
        process,
        agg_type
    )
    SELECT
      ARRAY_AGG(
        (
          first_bucket,
          last_bucket,
          num_buckets,
          metric,
          metric_type,
          key,
          process,
          agg_type,
          histogram_aggregates
        )
      )
    FROM
      aggregated_data
  )
);

WITH clients_histogram_aggregates_new AS (
  SELECT
    *
  FROM
    telemetry_derived.clients_histogram_aggregates_new_v1
  WHERE
    sample_id >= 0
    AND sample_id <= 99
),
clients_histogram_aggregates_partition AS (
  SELECT
    *
  FROM
    telemetry_derived.clients_histogram_aggregates_v2
),
clients_histogram_aggregates_old AS (
  SELECT
    submission_date,
    sample_id,
    client_id,
    os,
    app_version,
    app_build_id,
    hist_aggs.channel AS channel,
    CONCAT(client_id, os, app_version, app_build_id, hist_aggs.channel) AS join_key,
    histogram_aggregates
  FROM
    clients_histogram_aggregates_partition AS hist_aggs
  LEFT JOIN
    latest_versions
    ON latest_versions.channel = hist_aggs.channel
  WHERE
    app_version >= (latest_version - 2)
),
merged AS (
  SELECT
    old_data.submission_date AS old_sub_date,
    COALESCE(old_data.sample_id, new_data.sample_id) AS sample_id,
    COALESCE(old_data.client_id, new_data.client_id) AS client_id,
    COALESCE(old_data.os, new_data.os) AS os,
    COALESCE(old_data.app_version, CAST(new_data.app_version AS INT64)) AS app_version,
    COALESCE(old_data.app_build_id, new_data.app_build_id) AS app_build_id,
    COALESCE(old_data.channel, new_data.channel) AS channel,
    old_data.histogram_aggregates AS old_aggs,
    ARRAY(
      SELECT AS STRUCT
        first_bucket,
        last_bucket,
        num_buckets,
        metric,
        metric_type,
        key,
        process,
        agg_type,
        aggregates
      FROM
        UNNEST(new_data.histogram_aggregates)
    ) AS new_aggs
  FROM
    clients_histogram_aggregates_old AS old_data
  FULL OUTER JOIN
    clients_histogram_aggregates_new AS new_data
    ON new_data.join_key = old_data.join_key
)
SELECT
  @submission_date AS submission_date,
  sample_id,
  client_id,
  os,
  app_version,
  app_build_id,
  channel,
  CASE
    old_sub_date
    WHEN DATE_SUB(DATE(@submission_date), INTERVAL 1 DAY)
      THEN udf_merged_user_data(old_aggs, new_aggs)
    ELSE udf_use_old_data(old_aggs)
  END AS histogram_aggregates
FROM
  merged
