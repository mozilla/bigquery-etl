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
  FROM clients_daily_scalar_aggregates_v1
  WHERE submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
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
    agg_type,
    value
  FROM filtered_date_channel
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE value IS NOT NULL
),

version_filtered AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    scalar_aggs.channel AS channel,
    metric,
    metric_type,
    key,
    agg_type,
    value
  FROM filtered_aggregates AS scalar_aggs
  LEFT JOIN latest_versions
  ON latest_versions.channel = scalar_aggs.channel
  WHERE CAST(app_version AS INT64) >= (latest_version - 2)
),

scalar_aggregates AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type,
    CASE agg_type
      WHEN 'max' THEN max(value)
      WHEN 'min' THEN min(value)
      WHEN 'count' THEN sum(value)
      WHEN 'sum' THEN sum(value)
      WHEN 'false' THEN sum(value)
      WHEN 'true' THEN sum(value)
    END AS agg_value
  FROM version_filtered
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type
),

scalar_count_and_sum AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    'avg' AS agg_type,
    CASE WHEN agg_type = 'count' THEN agg_value ELSE 0 END AS count,
    CASE WHEN agg_type = 'sum' THEN agg_value ELSE 0 END AS sum
  FROM scalar_aggregates
  WHERE agg_type IN ('sum', 'count')),

scalar_averages AS (
  SELECT
    * EXCEPT(count, sum),
    SUM(sum) / SUM(count) AS avg
  FROM scalar_count_and_sum
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type),

scalars AS (
  SELECT *
  FROM scalar_aggregates
  WHERE metric_type in ("scalar", "keyed-scalar")

  UNION ALL

  SELECT *
  FROM scalar_averages),

all_booleans AS (
  SELECT
    *
  FROM
    scalar_aggregates
  WHERE
    metric_type in ("boolean", "keyed-scalar-boolean")
),

boolean_columns AS
  (SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type,
    CASE agg_type
      WHEN 'true' THEN agg_value ELSE 0
    END AS bool_true,
    CASE agg_type
      WHEN 'false' THEN agg_value ELSE 0
    END AS bool_false
  FROM all_booleans),

summed_bools AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      channel,
      metric,
      metric_type,
      key,
      '' AS agg_type,
      SUM(bool_true) AS bool_true,
      SUM(bool_false) AS bool_false
  FROM boolean_columns
  GROUP BY 1,2,3,4,5,6,7,8,9),

booleans AS
  (SELECT * EXCEPT(bool_true, bool_false),
  CASE
    WHEN bool_true > 0 AND bool_false > 0
    THEN "sometimes"
    WHEN bool_true > 0 AND bool_false = 0
    THEN "always"
    WHEN bool_true = 0 AND bool_false > 0
    THEN "never"
  END AS agg_value
  FROM summed_bools
  WHERE bool_true > 0 OR bool_false > 0)

SELECT
  *
FROM
  booleans

UNION ALL

SELECT
  * REPLACE(CAST(agg_value AS STRING) AS agg_value)
FROM
  scalars
WHERE
  metric_type in ("scalar", "keyed-scalar")
