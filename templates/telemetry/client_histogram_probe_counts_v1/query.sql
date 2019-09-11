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

CREATE TEMP FUNCTION udf_normalized_sum (arrs STRUCT<key_value ARRAY<STRUCT<key STRING, value INT64>>>)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Returns the normalized sum of the input maps.
  -- It returns the total_count[k] / SUM(total_count)
  -- for each key k.
  (
    WITH total_counts AS (
      SELECT
        sum(a.value) AS total_count
      FROM
        UNNEST(arrs.key_value) AS a
    ),

    summed_counts AS (
      SELECT
        a.key AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(arrs.key_value) AS a
      GROUP BY
        a.key
    ),

    final_values AS (
      SELECT
        STRUCT<key STRING, value FLOAT64>(k, 1.0 * v / total_count) AS record
      FROM
        summed_counts
      CROSS JOIN
        total_counts
    )

    SELECT
        ARRAY_AGG(record)
    FROM
      final_values
  )
);

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

normalized_histograms AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      hist_aggs.channel,
      bucket_range.first_bucket,
      bucket_range.last_bucket,
      bucket_range.num_buckets,
      aggregate.metric as metric,
      aggregate.metric_type AS metric_type,
      aggregate.key AS key,
      aggregate.agg_type as agg_type,
      latest_version,
      udf_normalized_sum(
        udf_aggregate_map_sum(ARRAY_AGG(STRUCT<key_value ARRAY<STRUCT <key STRING, value INT64>>>(aggregate.value)))) AS aggregates
  FROM
      clients_daily_histogram_aggregates_v1 AS hist_aggs
  CROSS JOIN
      UNNEST(histogram_aggregates) AS aggregate
  LEFT JOIN latest_versions
  ON latest_versions.channel = hist_aggs.channel
  WHERE ARRAY_LENGTH(value) > 0
  AND ((hist_aggs.channel = 'release' AND CAST(app_version AS INT64) >= (latest_version - 2))
  OR (hist_aggs.channel = 'beta' AND CAST(app_version AS INT64) >= (latest_version - 2))
  OR (hist_aggs.channel = 'nightly' AND CAST(app_version AS INT64) >= (latest_version - 2)))
  AND submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY
      client_id,
      os,
      app_version,
      app_build_id,
      channel,
      bucket_range.first_bucket,
      bucket_range.last_bucket,
      bucket_range.num_buckets,
      aggregate.metric,
      aggregate.metric_type,
      aggregate.key,
      aggregate.agg_type,
      latest_version),

bucketed_histograms AS
  (SELECT
      client_id,
      os,
      app_version,
      app_build_id,
      channel,
      first_bucket,
      last_bucket,
      num_buckets,
      metric,
      metric_type,
      normalized_histograms.key AS key,
      agg_type,
      agg.key AS bucket,
      agg.value AS value
  FROM normalized_histograms
  CROSS JOIN UNNEST(aggregates) AS agg
  WHERE num_buckets > 0),

clients_aggregates AS
  (SELECT
    os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    agg_type,
    bucket,
    STRUCT<key STRING, value FLOAT64>(
      CAST(bucket AS STRING),
      1.0 * SUM(value)
    ) AS record
  FROM bucketed_histograms
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    first_bucket,
    last_bucket,
    num_buckets,
    metric,
    metric_type,
    key,
    agg_type,
    bucket)

SELECT
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets

UNION ALL

SELECT
  CAST(NULL AS STRING) AS os,
  CAST(NULL AS STRING) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  agg_type,
  udf_fill_buckets(udf_dedupe_map_sum(
      ARRAY_AGG(record)
  ), udf_to_string_arr(udf_get_buckets(first_bucket, last_bucket, num_buckets, metric_type))) AS aggregates
FROM clients_aggregates
WHERE first_bucket IS NOT NULL
GROUP BY
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  agg_type,
  first_bucket,
  last_bucket,
  num_buckets
