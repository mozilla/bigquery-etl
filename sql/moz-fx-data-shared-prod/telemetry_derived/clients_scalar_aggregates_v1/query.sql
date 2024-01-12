WITH filtered_date_channel AS (
  SELECT
    * EXCEPT (app_version),
    CAST(app_version AS INT64) AS app_version
  FROM
    telemetry_derived.clients_daily_scalar_aggregates_v1
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
    metric,
    metric_type,
    key,
    process,
    agg_type,
    value
  FROM
    filtered_date_channel
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE
    value IS NOT NULL
),
version_filtered_new AS (
  SELECT
    submission_date,
    scalar_aggs.client_id,
    scalar_aggs.os,
    scalar_aggs.app_version,
    scalar_aggs.app_build_id,
    scalar_aggs.channel,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    value
  FROM
    filtered_aggregates AS scalar_aggs
  LEFT JOIN
    latest_versions
    USING (channel)
  WHERE
    app_version >= (latest_version - 2)
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
    --format:off
    CASE agg_type
      WHEN 'max' THEN max(value)
      WHEN 'min' THEN min(value)
      WHEN 'count' THEN sum(value)
      WHEN 'sum' THEN sum(value)
      WHEN 'false' THEN sum(value)
      WHEN 'true' THEN sum(value)
    END AS value
    --format:on
  FROM
    version_filtered_new
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
  FROM
    scalar_aggregates_new
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel
),
filtered_old AS (
  SELECT
    scalar_aggs.client_id,
    scalar_aggs.os,
    scalar_aggs.app_version,
    scalar_aggs.app_build_id,
    scalar_aggs.channel,
    scalar_aggregates
  FROM
    telemetry_derived.clients_scalar_aggregates_v1 AS scalar_aggs
  LEFT JOIN
    latest_versions
    USING (channel)
  WHERE
    app_version >= (latest_version - 2)
    AND submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
),
joined_new_old AS (
  SELECT
    COALESCE(old_data.client_id, new_data.client_id) AS client_id,
    COALESCE(old_data.os, new_data.os) AS os,
    COALESCE(old_data.app_version, new_data.app_version) AS app_version,
    COALESCE(old_data.app_build_id, new_data.app_build_id) AS app_build_id,
    COALESCE(old_data.channel, new_data.channel) AS channel,
    COALESCE(old_data.scalar_aggregates, []) AS old_aggs,
    COALESCE(new_data.scalar_aggregates, []) AS new_aggs
  FROM
    filtered_new AS new_data
  FULL OUTER JOIN
    filtered_old AS old_data
    USING (client_id, os, app_version, app_build_id, channel)
)
SELECT
  @submission_date AS submission_date,
  client_id,
  os,
  app_version,
  app_build_id,
  channel,
  udf.merge_scalar_user_data(ARRAY_CONCAT(old_aggs, new_aggs)) AS scalar_aggregates
FROM
  joined_new_old
