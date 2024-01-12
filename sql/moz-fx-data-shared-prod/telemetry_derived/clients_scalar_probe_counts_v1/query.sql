CREATE TEMP FUNCTION udf_boolean_buckets(
  scalar_aggs ARRAY<
    STRUCT<
      metric STRING,
      metric_type STRING,
      key STRING,
      process STRING,
      agg_type STRING,
      value FLOAT64
    >
  >
)
RETURNS ARRAY<
  STRUCT<
    metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    bucket STRING
  >
> AS (
  (
    WITH boolean_columns AS (
      SELECT
        metric,
        metric_type,
        key,
        process,
        agg_type,
        CASE
          agg_type
          WHEN 'true'
            THEN value
          ELSE 0
        END AS bool_true,
        CASE
          agg_type
          WHEN 'false'
            THEN value
          ELSE 0
        END AS bool_false
      FROM
        UNNEST(scalar_aggs)
      WHERE
        metric_type IN ("boolean", "keyed-scalar-boolean")
    ),
    summed_bools AS (
      SELECT
        metric,
        metric_type,
        key,
        process,
        '' AS agg_type,
        SUM(bool_true) AS bool_true,
        SUM(bool_false) AS bool_false
      FROM
        boolean_columns
      GROUP BY
        1,
        2,
        3,
        4
    ),
    booleans AS (
      SELECT
        * EXCEPT (bool_true, bool_false),
        CASE
          WHEN bool_true > 0
            AND bool_false > 0
            THEN "sometimes"
          WHEN bool_true > 0
            AND bool_false = 0
            THEN "always"
          WHEN bool_true = 0
            AND bool_false > 0
            THEN "never"
        END AS bucket
      FROM
        summed_bools
      WHERE
        bool_true > 0
        OR bool_false > 0
    )
    SELECT
      ARRAY_AGG((metric, metric_type, key, process, agg_type, bucket))
    FROM
      booleans
  )
);

WITH flat_clients_scalar_aggregates AS (
  SELECT
    *,
    os = 'Windows'
    AND channel = 'release' AS sampled,
  FROM
    clients_scalar_aggregates_v1
  WHERE
    submission_date = @submission_date
    AND (@app_version IS NULL OR app_version = @app_version)
),
log_min_max AS (
  SELECT
    metric,
    key,
    LOG(IF(MIN(value) <= 0, 1, MIN(value)), 2) log_min,
    LOG(IF(MAX(value) <= 0, 1, MAX(value)), 2) log_max
  FROM
    flat_clients_scalar_aggregates
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE
    metric_type = 'scalar'
    OR metric_type = 'keyed-scalar'
  GROUP BY
    1,
    2
),
buckets_by_metric AS (
  SELECT
    metric,
    key,
    ARRAY(
      SELECT
        FORMAT("%.*f", 2, bucket)
      FROM
        UNNEST(mozfun.glam.histogram_generate_scalar_buckets(log_min, log_max, 100)) AS bucket
      ORDER BY
        bucket
    ) AS buckets
  FROM
    log_min_max
),
static_combos AS (
  SELECT
    NULL AS os,
    NULL AS app_build_id
  UNION ALL
  SELECT
    NULL AS os,
    '*' AS app_build_id
  UNION ALL
  SELECT
    '*' AS os,
    NULL AS app_build_id
  UNION ALL
  SELECT
    '*' AS os,
    '*' AS app_build_id
),
all_combos AS (
  SELECT
    * EXCEPT (os, app_build_id),
    COALESCE(combos.os, flat_table.os) AS os,
    COALESCE(combos.app_build_id, flat_table.app_build_id) AS app_build_id
  FROM
    flat_clients_scalar_aggregates flat_table
  CROSS JOIN
    static_combos combos
),
user_aggregates AS (
  SELECT
    client_id,
    IF(os = '*', NULL, os) AS os,
    app_version,
    IF(app_build_id = '*', NULL, app_build_id) AS app_build_id,
    channel,
    IF(MAX(sampled), 10, 1) AS user_count,
    udf.merge_scalar_user_data(ARRAY_CONCAT_AGG(scalar_aggregates)) AS scalar_aggregates
  FROM
    all_combos
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel
),
build_ids AS (
  SELECT
    app_build_id,
    channel,
  FROM
    user_aggregates
  GROUP BY
    1,
    2
  HAVING
    -- Filter out builds having less than 0.5% of WAU
    -- for context see https://github.com/mozilla/glam/issues/1575#issuecomment-946880387
    CASE
      WHEN channel = 'release'
        THEN SUM(user_count) > 625000
      WHEN channel = 'beta'
        THEN SUM(user_count) > 9000
      WHEN channel = 'nightly'
        THEN SUM(user_count) > 375
      ELSE SUM(user_count) > 100
    END
),
bucketed_booleans AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    user_count,
    os = 'Windows'
    AND channel = 'release' AS sampled,
    udf_boolean_buckets(scalar_aggregates) AS scalar_aggregates
  FROM
    user_aggregates
),
bucketed_scalars AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    user_count,
    os = 'Windows'
    AND channel = 'release' AS sampled,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    -- Keep two decimal places before converting bucket to a string
    SAFE_CAST(
      FORMAT(
        "%.*f",
        2,
        mozfun.glam.histogram_bucket_from_value(buckets, SAFE_CAST(value AS FLOAT64)) + 0.0001
      ) AS STRING
    ) AS bucket
  FROM
    user_aggregates
  CROSS JOIN
    UNNEST(scalar_aggregates)
  LEFT JOIN
    buckets_by_metric
    USING (metric, key)
  WHERE
    metric_type = 'scalar'
    OR metric_type = 'keyed-scalar'
),
booleans_and_scalars AS (
  SELECT
    * EXCEPT (scalar_aggregates)
  FROM
    bucketed_booleans
  CROSS JOIN
    UNNEST(scalar_aggregates)
  UNION ALL
  SELECT
    *
  FROM
    bucketed_scalars
),
valid_booleans_scalars AS (
  SELECT *
  FROM booleans_and_scalars
  INNER JOIN
    build_ids
    USING (app_build_id, channel)
),
clients_scalar_bucket_counts AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    agg_type AS client_agg_type,
    'histogram' AS agg_type,
    bucket,
    SUM(user_count) AS user_count,
  FROM
    valid_booleans_scalars
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type,
    bucket
),
aggregated AS (
  SELECT
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    -- empty columns to match clients_histogram_probe_counts_v1 schema
    NULL AS first_bucket,
    NULL AS last_bucket,
    NULL AS num_buckets,
    client_agg_type,
    agg_type,
    SUM(user_count) AS total_users,
    CASE
      WHEN metric_type = 'scalar'
        OR metric_type = 'keyed-scalar'
        THEN mozfun.glam.histogram_fill_buckets(
            ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, user_count)),
            ANY_VALUE(buckets)
          )
      WHEN metric_type = 'boolean'
        OR metric_type = 'keyed-scalar-boolean'
        THEN mozfun.glam.histogram_fill_buckets(
            ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, user_count)),
            ['always', 'never', 'sometimes']
          )
    END AS aggregates
  FROM
    clients_scalar_bucket_counts
  LEFT JOIN
    buckets_by_metric
    USING (metric, key)
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type,
    agg_type
)
SELECT
  *,
  aggregates AS non_norm_aggregates
FROM
  aggregated
