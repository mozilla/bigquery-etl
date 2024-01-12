-- query for org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1;
CREATE TEMP FUNCTION udf_merged_user_data(
  aggs ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>>
)
RETURNS ARRAY<
  STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>
> AS (
  (
    WITH unnested AS (
      SELECT
        *
      FROM
        UNNEST(aggs)
      WHERE
        agg_type != "avg"
    ),
    aggregated AS (
      SELECT
        metric,
        metric_type,
        key,
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
        unnested
      WHERE
        value IS NOT NULL
      GROUP BY
        metric,
        metric_type,
        key,
        agg_type
    ),
    scalar_count_and_sum AS (
      SELECT
        metric,
        metric_type,
        key,
        'avg' AS agg_type,
        --format:off
        CASE WHEN agg_type = 'count' THEN value ELSE 0 END AS count,
        CASE WHEN agg_type = 'sum' THEN value ELSE 0 END AS sum
        --format:on
      FROM
        aggregated
      WHERE
        agg_type IN ('sum', 'count')
    ),
    scalar_averages AS (
      SELECT
        * EXCEPT (count, sum),
        SUM(sum) / SUM(count) AS agg_value
      FROM
        scalar_count_and_sum
      GROUP BY
        metric,
        metric_type,
        key,
        agg_type
    ),
    merged_data AS (
      SELECT
        *
      FROM
        aggregated
      UNION ALL
      SELECT
        *
      FROM
        scalar_averages
    )
    SELECT
      ARRAY_AGG((metric, metric_type, key, agg_type, value))
    FROM
      merged_data
  )
);

WITH filtered_date_channel AS (
  SELECT
    *
  FROM
    glam_etl.org_mozilla_fenix_glam_nightly__view_clients_daily_scalar_aggregates_v1
  WHERE
    submission_date = @submission_date
),
filtered_aggregates AS (
  SELECT
    submission_date,
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
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
    scalar_aggs.ping_type,
    scalar_aggs.os,
    scalar_aggs.app_version,
    scalar_aggs.app_build_id,
    scalar_aggs.channel,
    metric,
    metric_type,
    key,
    agg_type,
    value
  FROM
    filtered_aggregates AS scalar_aggs
  LEFT JOIN
    glam_etl.org_mozilla_fenix_glam_nightly__latest_versions_v1
    USING (channel)
  WHERE
      -- allow for builds to be slighly ahead of the current submission date, to
      -- account for a reasonable amount of clock skew
    mozfun.glam.build_hour_to_datetime(app_build_id) < DATE_ADD(@submission_date, INTERVAL 3 day)
      -- only keep builds from the last year
    AND mozfun.glam.build_hour_to_datetime(app_build_id) > DATE_SUB(
      @submission_date,
      INTERVAL 365 day
    )
    AND app_version > (latest_version - 3)
),
scalar_aggregates_new AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
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
  WHERE
    -- avoid overflows from very large numbers that are typically anomalies
    value <= POW(2, 40)
  GROUP BY
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type
),
filtered_new AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    ARRAY_AGG((metric, metric_type, key, agg_type, value)) AS scalar_aggregates
  FROM
    scalar_aggregates_new
  GROUP BY
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel
),
filtered_old AS (
  SELECT
    scalar_aggs.client_id,
    scalar_aggs.ping_type,
    scalar_aggs.os,
    scalar_aggs.app_version,
    scalar_aggs.app_build_id,
    scalar_aggs.channel,
    scalar_aggregates
  FROM
    glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1 AS scalar_aggs
  LEFT JOIN
    glam_etl.org_mozilla_fenix_glam_nightly__latest_versions_v1
    USING (channel)
  WHERE
      -- allow for builds to be slighly ahead of the current submission date, to
      -- account for a reasonable amount of clock skew
    mozfun.glam.build_hour_to_datetime(app_build_id) < DATE_ADD(@submission_date, INTERVAL 3 day)
      -- only keep builds from the last year
    AND mozfun.glam.build_hour_to_datetime(app_build_id) > DATE_SUB(
      @submission_date,
      INTERVAL 365 day
    )
    AND app_version > (latest_version - 3)
),
joined_new_old AS (
  SELECT
    COALESCE(old_data.client_id, new_data.client_id) AS client_id,
    COALESCE(old_data.ping_type, new_data.ping_type) AS ping_type,
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
    USING (client_id, ping_type, os, app_version, app_build_id, channel)
)
SELECT
  client_id,
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  udf_merged_user_data(ARRAY_CONCAT(old_aggs, new_aggs)) AS scalar_aggregates
FROM
  joined_new_old
