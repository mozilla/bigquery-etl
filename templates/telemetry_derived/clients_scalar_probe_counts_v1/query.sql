CREATE TEMP FUNCTION udf_get_buckets()
RETURNS ARRAY<STRING> AS (
  (
    SELECT ARRAY_AGG(CAST(bucket AS STRING))
    FROM UNNEST([0, 1.0, 41.0, 81.0, 121.0, 162.0, 202.0, 242.0, 283.0, 323.0, 363.0, 404.0, 444.0, 484.0, 525.0, 565.0, 605.0, 646.0, 686.0, 726.0, 767.0, 807.0, 847.0, 888.0, 928.0, 968.0, 1008.0, 1049.0, 1089.0, 1129.0, 1170.0, 1210.0, 1250.0, 1291.0, 1331.0, 1371.0, 1412.0, 1452.0, 1492.0, 1533.0, 1573.0, 1613.0, 1654.0, 1694.0, 1734.0, 1775.0, 1815.0, 1855.0, 1895.0, 1936.0, 1976.0, 2016.0, 2057.0, 2097.0, 2137.0, 2178.0, 2218.0, 2258.0, 2299.0, 2339.0, 2379.0, 2420.0, 2460.0, 2500.0, 2541.0, 2581.0, 2621.0, 2662.0, 2702.0, 2742.0, 2782.0, 2823.0, 2863.0, 2903.0, 2944.0, 2984.0, 3024.0, 3065.0, 3105.0, 3145.0, 3186.0, 3226.0, 3266.0, 3307.0, 3347.0, 3387.0, 3428.0, 3468.0, 3508.0, 3549.0, 3589.0, 3629.0, 3669.0, 3710.0, 3750.0, 3790.0, 3831.0, 3871.0, 3911.0, 3952.0, 3992.0, 4032.0, 4073.0, 4113.0, 4153.0, 4194.0, 4234.0, 4274.0, 4315.0, 4355.0, 4395.0, 4436.0, 4476.0, 4516.0, 4556.0, 4597.0, 4637.0, 4677.0, 4718.0, 4758.0, 4798.0, 4839.0, 4879.0, 4919.0, 4960.0, 5000.0, 5040.0, 5081.0, 5121.0, 5161.0, 5202.0, 5242.0, 5282.0, 5323.0, 5363.0, 5403.0, 5444.0, 5484.0, 5524.0, 5564.0, 5605.0, 5645.0, 5685.0, 5726.0, 5766.0, 5806.0, 5847.0, 5887.0, 5927.0, 5968.0, 6008.0, 6048.0, 6089.0, 6129.0, 6169.0, 6210.0, 6250.0, 6290.0, 6331.0, 6371.0, 6411.0, 6451.0, 6492.0, 6532.0, 6572.0, 6613.0, 6653.0, 6693.0, 6734.0, 6774.0, 6814.0, 6855.0, 6895.0, 6935.0, 6976.0, 7016.0, 7056.0, 7097.0, 7137.0, 7177.0, 7218.0, 7258.0, 7298.0, 7338.0, 7379.0, 7419.0, 7459.0, 7500.0, 7540.0, 7580.0, 7621.0, 7661.0, 7701.0, 7742.0, 7782.0, 7822.0, 7863.0, 7903.0, 7943.0, 7984.0, 8024.0, 8064.0, 8105.0, 8145.0, 8185.0, 8225.0, 8266.0, 8306.0, 8346.0, 8387.0, 8427.0, 8467.0, 8508.0, 8548.0, 8588.0, 8629.0, 8669.0, 8709.0, 8750.0, 8790.0, 8830.0, 8871.0, 8911.0, 8951.0, 8992.0, 9032.0, 9072.0, 9112.0, 9153.0, 9193.0, 9233.0, 9274.0, 9314.0, 9354.0, 9395.0, 9435.0, 9475.0, 9516.0, 9556.0, 9596.0, 9637.0, 9677.0, 9717.0, 9758.0, 9798.0, 9838.0, 9879.0, 9919.0, 9959.0, 10000.0]) AS bucket
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
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  CAST(NULL AS INT64) AS app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
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
  CAST(NULL AS INT64) AS app_version,
  CAST(NULL AS STRING) AS app_build_id,
  CAST(NULL AS STRING) AS channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type,
  SUM(count) AS total_users,
  CASE
    WHEN metric_type = 'scalar' OR metric_type = 'keyed-scalar'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      udf_get_buckets()
    )
    WHEN metric_type = 'boolean' OR metric_type = 'keyed-scalar-boolean'
    THEN udf_fill_buckets(
      udf_dedupe_map_sum(ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, count))),
      ['always','never','sometimes'])
   END AS aggregates
FROM
  clients_scalar_bucket_counts_v1
GROUP BY
  metric,
  metric_type,
  key,
  client_agg_type,
  agg_type
