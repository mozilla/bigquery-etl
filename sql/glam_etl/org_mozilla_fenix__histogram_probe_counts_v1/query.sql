CREATE TEMP FUNCTION udf_exponential_buckets(min FLOAT64, max FLOAT64, nBuckets FLOAT64)
RETURNS ARRAY<FLOAT64>
LANGUAGE js
AS
  '''
  let logMax = Math.log(max);
  let current = min;
  if (current === 0) {
    current = 1;
  } // If starting from 0, the second bucket should be 1 rather than 0
  let retArray = [0, current];
  for (let bucketIndex = 2; bucketIndex < Math.min(nBuckets, max, 10000); bucketIndex++) {
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
LANGUAGE js
AS
  '''
  let result = [0];
  for (let i = 1; i < Math.min(nBuckets, max, 10000); i++) {
    let linearRange = (min * (nBuckets - 1 - i) + max * (i - 1)) / (nBuckets - 2);
    result.push(Math.round(linearRange));
  }
  return result;
''';

CREATE TEMP FUNCTION udf_functional_buckets(
  log_base INT64,
  buckets_per_magnitude INT64,
  range_max INT64
)
RETURNS ARRAY<FLOAT64>
LANGUAGE js
AS
  '''
  function sample_to_bucket_index(sample) {
    // Get the index of the sample
    // https://github.com/mozilla/glean/blob/master/glean-core/src/histogram/functional.rs
    let exponent = Math.pow(log_base, 1.0/buckets_per_magnitude);
    return Math.ceil(Math.log(sample + 1) / Math.log(exponent));
  }

  let buckets = new Set([0]);
  for (let index = 0; index < sample_to_bucket_index(range_max); index++) {
    // Avoid re-using the exponent due to floating point issues when carrying
    // the `pow` operation e.g. `let exponent = ...; Math.pow(exponent, index)`.
    let bucket = Math.floor(Math.pow(log_base, index/buckets_per_magnitude));
    buckets.add(bucket);
  }

  return [...buckets]
''';

CREATE TEMP FUNCTION udf_to_string_arr(buckets ARRAY<INT64>)
RETURNS ARRAY<STRING> AS (
  (SELECT ARRAY_AGG(CAST(bucket AS STRING)) FROM UNNEST(buckets) AS bucket)
);

CREATE TEMP FUNCTION udf_get_buckets(
  metric_type STRING,
  range_min INT64,
  range_max INT64,
  bucket_count INT64
)
RETURNS ARRAY<INT64> AS (
  (
    WITH buckets AS (
      SELECT
        CASE
        WHEN
          metric_type = 'timing_distribution'
        THEN
          -- https://mozilla.github.io/glean/book/user/metrics/timing_distribution.html
          udf_functional_buckets(2, 8, range_max)
        WHEN
          metric_type = 'memory_distribution'
        THEN
          -- https://mozilla.github.io/glean/book/user/metrics/memory_distribution.html
          udf_functional_buckets(2, 16, range_max)
        WHEN
          metric_type = 'custom_distribution_exponential'
        THEN
          udf_exponential_buckets(range_min, range_max, bucket_count)
        WHEN
          metric_type = 'custom_distribution_linear'
        THEN
          udf_linear_buckets(range_min, range_max, bucket_count)
        ELSE
          []
        END
        AS arr
    )
    SELECT
      ARRAY_AGG(CAST(item AS INT64))
    FROM
      buckets
    CROSS JOIN
      UNNEST(arr) AS item
  )
);

CREATE TEMP FUNCTION udf_dedupe_map_sum(map ARRAY<STRUCT<key STRING, value FLOAT64>>)
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

CREATE TEMP FUNCTION udf_buckets_to_map(buckets ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given an array of values, transform them into a histogram MAP
  -- with the number of each key in the `buckets` array
  (SELECT ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, 1.0)) FROM UNNEST(buckets) AS bucket)
);

CREATE TEMP FUNCTION udf_fill_buckets(
  input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
  buckets ARRAY<STRING>
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  (
    WITH total_counts AS (
      SELECT
        key,
        COALESCE(e.value, 0.0) AS value
      FROM
        UNNEST(buckets) AS key
      LEFT JOIN
        UNNEST(input_map) AS e
      ON
        SAFE_CAST(key AS STRING) = e.key
    )
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(SAFE_CAST(key AS STRING), value))
    FROM
      total_counts
  )
);

SELECT
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
UNION ALL
SELECT
  ping_type,
  os,
  app_version,
  NULL AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  ping_type,
  os,
  app_version,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
UNION ALL
SELECT
  ping_type,
  NULL AS os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  ping_type,
  app_version,
  app_build_id,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
UNION ALL
SELECT
  NULL AS ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
UNION ALL
SELECT
  ping_type,
  NULL AS os,
  app_version,
  NULL AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  ping_type,
  app_version,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
UNION ALL
SELECT
  NULL AS ping_type,
  os,
  app_version,
  NULL AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  os,
  app_version,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
UNION ALL
SELECT
  NULL AS ping_type,
  NULL AS os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  app_version,
  app_build_id,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
UNION ALL
SELECT
  NULL AS ping_type,
  NULL AS os,
  app_version,
  NULL AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type AS client_agg_type,
  'histogram' AS agg_type,
  CAST(ROUND(SUM(record.value)) AS INT64) AS total_users,
  udf_fill_buckets(
    udf_dedupe_map_sum(ARRAY_AGG(record)),
    udf_to_string_arr(udf_get_buckets(metric_type, range_min, range_max, bucket_count))
  ) AS aggregates
FROM
  glam_etl.org_mozilla_fenix__histogram_bucket_counts_v1
GROUP BY
  app_version,
  channel,
  range_min,
  range_max,
  bucket_count,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
