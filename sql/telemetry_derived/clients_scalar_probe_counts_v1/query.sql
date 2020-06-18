CREATE TEMP FUNCTION udf_fill_buckets(input_map ARRAY<STRUCT<key STRING, value FLOAT64>>, buckets ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  (
    WITH total_counts AS (
      SELECT
        key,
        COALESCE(e.value, 0.0) AS value
      FROM
        UNNEST(buckets) as key
      LEFT JOIN
        UNNEST(input_map) AS e ON SAFE_CAST(key AS STRING) = e.key
    )
    
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(SAFE_CAST(key AS STRING), value))
    FROM
      total_counts
  )
);

CREATE TEMPORARY FUNCTION udf_generate_buckets(min_bucket FLOAT64, max_bucket FLOAT64, num_buckets INT64)
RETURNS ARRAY<STRING>
LANGUAGE js
AS
'''
  let bucket_size = (max_bucket - min_bucket) / num_buckets;
  let buckets = new Set();
  for (let bucket = min_bucket; bucket < max_bucket; bucket += bucket_size) {
    buckets.add(Math.pow(2, bucket).toFixed(2));
  }
  return Array.from(buckets);
''';

CREATE TEMP FUNCTION udf_get_values(required ARRAY<FLOAT64>, values ARRAY<FLOAT64>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  (
    SELECT ARRAY_AGG(record)
    FROM (
      SELECT
        STRUCT<key STRING, value FLOAT64>(
          CAST(k AS STRING),
          values[OFFSET(CAST(k AS INT64))]
        ) as record
      FROM
        UNNEST(required) AS k
    )
  )
);

CREATE TEMP FUNCTION udf_bucket (
  buckets ARRAY<STRING>,
  val FLOAT64
)
RETURNS FLOAT64 AS (
  -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
  (
    SELECT MAX(CAST(bucket AS FLOAT64))
    FROM UNNEST(buckets) AS bucket
    WHERE val >= CAST(bucket AS FLOAT64)
  )
);

CREATE TEMP FUNCTION udf_boolean_buckets(
  scalar_aggs ARRAY<STRUCT<metric STRING, metric_type STRING, key STRING, process STRING, agg_type STRING, value FLOAT64>>)
  RETURNS ARRAY<STRUCT<metric STRING,
    metric_type STRING,
    key STRING,
    process STRING,
    agg_type STRING,
    bucket STRING>> AS (
    (
      WITH boolean_columns AS
        (SELECT
          metric,
          metric_type,
          key,
          process,
          agg_type,
          CASE agg_type
            WHEN 'true' THEN value ELSE 0
          END AS bool_true,
          CASE agg_type
            WHEN 'false' THEN value ELSE 0
          END AS bool_false
        FROM UNNEST(scalar_aggs)
        WHERE metric_type in ("boolean", "keyed-scalar-boolean")),

      summed_bools AS
        (SELECT
          metric,
          metric_type,
          key,
          process,
          '' AS agg_type,
          SUM(bool_true) AS bool_true,
          SUM(bool_false) AS bool_false
        FROM boolean_columns
        GROUP BY 1,2,3,4),

      booleans AS
        (SELECT * EXCEPT(bool_true, bool_false),
        CASE
          WHEN bool_true > 0 AND bool_false > 0
          THEN "sometimes"
          WHEN bool_true > 0 AND bool_false = 0
          THEN "always"
          WHEN bool_true = 0 AND bool_false > 0
          THEN "never"
        END AS bucket
        FROM summed_bools
        WHERE bool_true > 0 OR bool_false > 0)

      SELECT ARRAY_AGG((metric, metric_type, key, process, agg_type, bucket))
      FROM booleans
    )
);

WITH flat_clients_scalar_aggregates AS (
  SELECT *,
    os = 'Windows' and channel = 'release' AS sampled,
  FROM clients_scalar_aggregates_v1),

log_min_max AS (
  SELECT
    metric,
    key,
    LOG(IF(MIN(value) <= 0, 1, MIN(value)), 2) log_min,
    LOG(IF(MAX(value) <= 0, 1, MAX(value)), 2) log_max
  FROM
    flat_clients_scalar_aggregates
    CROSS JOIN UNNEST(scalar_aggregates)
  WHERE
    metric_type = 'scalar' OR metric_type = 'keyed-scalar'
  GROUP BY 1, 2),

buckets_by_metric AS (
  SELECT metric, key, udf_generate_buckets(log_min, log_max, 100) AS buckets
  FROM log_min_max),

static_combos as (
  SELECT null as os, null as app_build_id
  UNION ALL
  SELECT null as os, '*' as app_build_id
  UNION ALL
  SELECT '*' as os, null as app_build_id
  UNION ALL
  SELECT '*' as os, '*' as app_build_id
),

all_combos AS (
  SELECT
    * EXCEPT(os, app_build_id),
    COALESCE(combos.os, flat_table.os) as os,
    COALESCE(combos.app_build_id, flat_table.app_build_id) as app_build_id
  FROM
     flat_clients_scalar_aggregates flat_table
  CROSS JOIN
     static_combos combos),

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
    channel),

bucketed_booleans AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    user_count,
    os = 'Windows' and channel = 'release' AS sampled,
    udf_boolean_buckets(scalar_aggregates) AS scalar_aggregates
  FROM
    user_aggregates),

bucketed_scalars AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    user_count,
    os = 'Windows' and channel = 'release' AS sampled,
    metric,
    metric_type,
    key,
    process,
    agg_type,
    -- Keep two decimal places before converting bucket to a string
    SAFE_CAST(FORMAT("%.*f", 2, udf_bucket(buckets, SAFE_CAST(value AS FLOAT64)) + 0.0001) AS STRING) AS bucket
  FROM
    user_aggregates
  CROSS JOIN UNNEST(scalar_aggregates)
  LEFT JOIN buckets_by_metric
    USING(metric, key)
  WHERE
    metric_type = 'scalar' OR metric_type = 'keyed-scalar'),

booleans_and_scalars AS (
  SELECT * EXCEPT(scalar_aggregates)
  FROM bucketed_booleans
  CROSS JOIN UNNEST(scalar_aggregates)

  UNION ALL

  SELECT *
  FROM bucketed_scalars),

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
  FROM booleans_and_scalars
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
    bucket)

SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  process,
  client_agg_type,
  agg_type,
  SUM(user_count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, user_count)),
      ANY_VALUE(buckets)
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, user_count)),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts
LEFT JOIN buckets_by_metric
  USING(metric, key)
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
