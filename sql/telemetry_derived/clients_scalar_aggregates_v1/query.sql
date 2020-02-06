CREATE TEMP FUNCTION udf_merged_user_data(
  old_aggs ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, process STRING, agg_type STRING, value FLOAT64>>,
  new_aggs ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, process STRING, agg_type STRING, value FLOAT64>>)

  RETURNS ARRAY<STRUCT<metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    value FLOAT64>> AS (
  (
    WITH unnested AS
      (SELECT *
      FROM UNNEST(old_aggs)
      WHERE agg_type != "avg"

      UNION ALL

      SELECT *
      FROM UNNEST(new_aggs)),

    aggregated AS (
      SELECT
        metric,
        metric_type,
        key,
        process,
        agg_type,
        CASE agg_type
          WHEN 'max' THEN max(value)
          WHEN 'min' THEN min(value)
          WHEN 'count' THEN sum(value)
          WHEN 'sum' THEN sum(value)
          WHEN 'false' THEN sum(value)
          WHEN 'true' THEN sum(value)
        END AS value
      FROM unnested
      WHERE value IS NOT NULL
      GROUP BY
        metric,
        metric_type,
        key,
        process,
        agg_type),

    scalar_count_and_sum AS (
      SELECT
        metric,
        metric_type,
        key,
        process,
        'avg' AS agg_type,
        CASE WHEN agg_type = 'count' THEN value ELSE 0 END AS count,
        CASE WHEN agg_type = 'sum' THEN value ELSE 0 END AS sum
      FROM aggregated
      WHERE agg_type IN ('sum', 'count')),

    scalar_averages AS (
      SELECT
        * EXCEPT(count, sum),
        SUM(sum) / SUM(count) AS agg_value
      FROM scalar_count_and_sum
      GROUP BY
        metric,
        metric_type,
        key,
        process,
        agg_type),

    merged_data AS (
      SELECT *
      FROM aggregated

      UNION ALL

      SELECT *
      FROM scalar_averages)

    SELECT ARRAY_AGG((metric, metric_type, key, process, agg_type, value))
    FROM merged_data
  )
);

WITH filtered_date_channel AS (
  SELECT *
  FROM clients_daily_scalar_aggregates_v1
  WHERE submission_date = @submission_date
),

filtered_aggregates AS (
  SELECT
    submission_date,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    value
  FROM filtered_date_channel
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE value IS NOT NULL
),

version_filtered_new AS (
  SELECT
    submission_date,
    client_id,
    os,
    app_version,
    app_build_id,
    scalar_aggs.channel AS channel,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    value
  FROM filtered_aggregates AS scalar_aggs
  LEFT JOIN latest_versions
  ON latest_versions.channel = scalar_aggs.channel
  WHERE CAST(app_version AS INT64) >= (latest_version - 2)
),

scalar_aggregates_new AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    CASE agg_type
      WHEN 'max' THEN max(value)
      WHEN 'min' THEN min(value)
      WHEN 'count' THEN sum(value)
      WHEN 'sum' THEN sum(value)
      WHEN 'false' THEN sum(value)
      WHEN 'true' THEN sum(value)
    END AS value
  FROM version_filtered_new
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type
),

filtered_new AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    ARRAY_AGG((metric, metric_type, key, process, agg_type, value)) AS scalar_aggregates
  FROM scalar_aggregates_new
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel
),

filtered_old AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    scalar_aggs.channel AS channel,
    scalar_aggregates
  FROM clients_scalar_aggregates_v1 AS scalar_aggs
  LEFT JOIN latest_versions
  ON latest_versions.channel = scalar_aggs.channel
  WHERE app_version >= (latest_version - 2)
),

joined_new_old AS (
  SELECT
    COALESCE(old_data.client_id, new_data.client_id) AS client_id,
    COALESCE(old_data.os, new_data.os) AS os,
    COALESCE(old_data.app_version, CAST(new_data.app_version AS INT64)) AS app_version,
    COALESCE(old_data.app_build_id, new_data.app_build_id) AS app_build_id,
    COALESCE(old_data.channel, new_data.channel) AS channel,
    old_data.scalar_aggregates AS old_aggs,
    new_data.scalar_aggregates AS new_aggs
  FROM filtered_new AS new_data
  FULL OUTER JOIN filtered_old AS old_data
    ON new_data.client_id = old_data.client_id
    AND new_data.os = old_data.os
    AND CAST(new_data.app_version AS INT64) = old_data.app_version
    AND new_data.app_build_id = old_data.app_build_id
    AND new_data.channel = old_data.channel)

SELECT
  client_id,
  os,
  app_version,
  app_build_id,
  channel,
  udf_merged_user_data(old_aggs, new_aggs) AS scalar_aggregates
FROM joined_new_old
