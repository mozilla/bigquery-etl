CREATE TEMP FUNCTION udf_exponential_buckets(min INT64, max INT64, nBuckets INT64)
RETURNS ARRAY<FLOAT64>
LANGUAGE js AS
'''
  let logMax = Math.log(max);
  let current = min;
  if (current === 0) {
    current = 1;
  } // If starting from 0, the second bucket should be 1 rather than 0
  let retArray = [0, current];
  for (let bucketIndex = 2; bucketIndex < nBuckets; bucketIndex++) {
    let logCurrent = Math.log(current);
    let logRatio = (logMax - logCurrent) / (nBuckets - bucketIndex);
    let logNext = logCurrent + logRatio;
    let nextValue =  Math.round(Math.exp(logNext));
    current = nextValue > current ? nextValue : current + 1;
    retArray[bucketIndex] = current;
  }
  return retArray
''';

CREATE TEMP FUNCTION udf_linear_buckets(min INT64, max INT64, nBuckets INT64)
RETURNS ARRAY<FLOAT64>
LANGUAGE js AS
'''
  let result = [0];
  for (let i = 1; i < nBuckets; i++) {
    let linearRange = (min * (nBuckets - 1 - i) + max * (i - 1)) / (nBuckets - 2);
    result.push(Math.round(linearRange));
  }
  return result;
''';

CREATE TEMP FUNCTION udf_to_string_arr(buckets ARRAY<INT64>)
RETURNS ARRAY<STRING> AS (
  (
    SELECT ARRAY_AGG(CAST(bucket AS STRING))
    FROM UNNEST(buckets) AS bucket
  )
);


CREATE TEMP FUNCTION udf_get_buckets(min INT64, max INT64, num INT64, metric_type STRING)
RETURNS ARRAY<INT64> AS (
  (
    WITH buckets AS (
      SELECT
        CASE
          WHEN metric_type = 'histogram-exponential'
          THEN udf_exponential_buckets(min, max, num)
          ELSE udf_linear_buckets(min, max, num)
       END AS arr
    )

    SELECT ARRAY_AGG(CAST(item AS INT64))
    FROM buckets
    CROSS JOIN UNNEST(arr) AS item
  )
);

CREATE TEMP FUNCTION udf_bucket (
  val FLOAT64,
  min_bucket INT64,
  max_bucket INT64,
  num_buckets INT64,
  metric_type STRING
)
RETURNS FLOAT64 AS (
  -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
  (
    SELECT max(CAST(bucket AS INT64))
    FROM UNNEST(udf_get_buckets(min_bucket, max_bucket, num_buckets, metric_type)) AS bucket
    WHERE val >= CAST(bucket AS INT64)
  )
);

CREATE TEMP FUNCTION udf_dedupe_map_sum (map ARRAY<STRUCT<key STRING, value FLOAT64>>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP with duplicate keys, de-duplicates by summing the values of duplicate keys
  (
    WITH summed_counts AS (
      SELECT
        STRUCT<key STRING, value FLOAT64>(e.key, SUM(e.value)) AS record
      FROM
        UNNEST(map) AS e
      GROUP BY
        e.key
    )

    SELECT
       ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(record.key, record.value))
    FROM
      summed_counts
  )
);

CREATE TEMP FUNCTION udf_buckets_to_map (buckets ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given an array of values, transform them into a histogram MAP
  -- with the number of each key in the `buckets` array
  (
    SELECT
       ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, 1.0))
    FROM
      UNNEST(buckets) AS bucket
  )
);

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

WITH latest_versions AS (
  SELECT channel, MAX(CAST(app_version AS INT64)) AS latest_version
  FROM
    (SELECT
      normalized_channel AS channel,
      SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
      COUNT(*)
    FROM `moz-fx-data-shared-prod.telemetry.main`
    WHERE DATE(submission_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND normalized_channel IN ("nightly", "beta", "release")
    GROUP BY 1, 2
    HAVING COUNT(*) > 1000
    ORDER BY 1, 2 DESC)
  GROUP BY 1),

scalar_aggregates AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    scalar_aggs.channel,
    metric,
    metric_type,
    key,
    agg_type,
    CASE agg_type
      WHEN 'max' THEN max(value)
      WHEN 'min' THEN min(value)
      WHEN 'avg' THEN avg(value)
      WHEN 'sum' THEN sum(value)
      WHEN 'false' THEN sum(value)
      WHEN 'true' THEN sum(value)
    END AS agg_value
  FROM
    clients_daily_scalar_aggregates_v1 AS scalar_aggs
  CROSS JOIN
    UNNEST(scalar_aggregates)
  LEFT JOIN latest_versions
  ON latest_versions.channel = scalar_aggs.channel
  WHERE value IS NOT NULL
  AND ((scalar_aggs.channel = 'release' AND CAST(app_version AS INT64) >= (latest_version - 2))
  OR (scalar_aggs.channel = 'beta' AND CAST(app_version AS INT64) >= (latest_version - 2))
  OR (scalar_aggs.channel = 'nightly' AND CAST(app_version AS INT64) >= (latest_version - 2)))
  AND submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
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
  WHERE bool_true > 0 OR bool_false > 0),

clients_aggregates_v1 AS (
  SELECT
    *
  FROM
    booleans

  UNION ALL

  SELECT
    * REPLACE(CAST(agg_value AS STRING) AS agg_value)
  FROM
    scalar_aggregates
  WHERE
    metric_type in ("scalar", "keyed-scalar")
),

bucketed_scalars AS (
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
    CASE
      WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
      THEN SAFE_CAST(udf_bucket(SAFE_CAST(agg_value AS FLOAT64), 0, 1000, 50, 'scalar') AS STRING)
      WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
      THEN agg_value
    END AS bucket
  FROM
    clients_aggregates_v1
)

SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) as os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  app_version,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  app_version,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  app_version,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  os,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  channel,
  metric,
  metric_type,
  key,
  client_agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      udf_to_string_arr(udf_get_buckets(0, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(udf_buckets_to_map(ARRAY_AGG(bucket))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucketed_scalars
GROUP BY
  metric,
  metric_type,
  key,
  client_agg_type
