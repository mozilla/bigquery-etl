CREATE TEMP FUNCTION udf_exponential_buckets(min FLOAT64, max FLOAT64, nBuckets FLOAT64)
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

CREATE TEMP FUNCTION udf_linear_buckets(min FLOAT64, max FLOAT64, nBuckets FLOAT64)
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

WITH bucketed_scalars AS (
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
        THEN SAFE_CAST(udf_bucket(SAFE_CAST(agg_value AS FLOAT64), 1, 1000, 50, 'scalar') AS STRING)
      WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
        THEN agg_value
    END AS bucket
  FROM
    clients_scalar_aggregates_v1),

bucket_counts AS (
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
    bucket,
    COUNT(*) AS count
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
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) as os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  os,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  os,
  app_version,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  os,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  app_version,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  app_version,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  os,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_to_string_arr(udf_get_buckets(1, 1000, 50, metric_type))
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  bucket_counts
GROUP BY
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
