CREATE TEMP FUNCTION udf_bucket(val FLOAT64)
RETURNS FLOAT64 AS (
  -- Bucket `value` into a histogram with min_bucket, max_bucket and num_buckets
  (
    SELECT
      max(CAST(bucket AS INT64))
    FROM
      UNNEST(
        [
          0,
          1.0,
          41.0,
          81.0,
          121.0,
          162.0,
          202.0,
          242.0,
          283.0,
          323.0,
          363.0,
          404.0,
          444.0,
          484.0,
          525.0,
          565.0,
          605.0,
          646.0,
          686.0,
          726.0,
          767.0,
          807.0,
          847.0,
          888.0,
          928.0,
          968.0,
          1008.0,
          1049.0,
          1089.0,
          1129.0,
          1170.0,
          1210.0,
          1250.0,
          1291.0,
          1331.0,
          1371.0,
          1412.0,
          1452.0,
          1492.0,
          1533.0,
          1573.0,
          1613.0,
          1654.0,
          1694.0,
          1734.0,
          1775.0,
          1815.0,
          1855.0,
          1895.0,
          1936.0,
          1976.0,
          2016.0,
          2057.0,
          2097.0,
          2137.0,
          2178.0,
          2218.0,
          2258.0,
          2299.0,
          2339.0,
          2379.0,
          2420.0,
          2460.0,
          2500.0,
          2541.0,
          2581.0,
          2621.0,
          2662.0,
          2702.0,
          2742.0,
          2782.0,
          2823.0,
          2863.0,
          2903.0,
          2944.0,
          2984.0,
          3024.0,
          3065.0,
          3105.0,
          3145.0,
          3186.0,
          3226.0,
          3266.0,
          3307.0,
          3347.0,
          3387.0,
          3428.0,
          3468.0,
          3508.0,
          3549.0,
          3589.0,
          3629.0,
          3669.0,
          3710.0,
          3750.0,
          3790.0,
          3831.0,
          3871.0,
          3911.0,
          3952.0,
          3992.0,
          4032.0,
          4073.0,
          4113.0,
          4153.0,
          4194.0,
          4234.0,
          4274.0,
          4315.0,
          4355.0,
          4395.0,
          4436.0,
          4476.0,
          4516.0,
          4556.0,
          4597.0,
          4637.0,
          4677.0,
          4718.0,
          4758.0,
          4798.0,
          4839.0,
          4879.0,
          4919.0,
          4960.0,
          5000.0,
          5040.0,
          5081.0,
          5121.0,
          5161.0,
          5202.0,
          5242.0,
          5282.0,
          5323.0,
          5363.0,
          5403.0,
          5444.0,
          5484.0,
          5524.0,
          5564.0,
          5605.0,
          5645.0,
          5685.0,
          5726.0,
          5766.0,
          5806.0,
          5847.0,
          5887.0,
          5927.0,
          5968.0,
          6008.0,
          6048.0,
          6089.0,
          6129.0,
          6169.0,
          6210.0,
          6250.0,
          6290.0,
          6331.0,
          6371.0,
          6411.0,
          6451.0,
          6492.0,
          6532.0,
          6572.0,
          6613.0,
          6653.0,
          6693.0,
          6734.0,
          6774.0,
          6814.0,
          6855.0,
          6895.0,
          6935.0,
          6976.0,
          7016.0,
          7056.0,
          7097.0,
          7137.0,
          7177.0,
          7218.0,
          7258.0,
          7298.0,
          7338.0,
          7379.0,
          7419.0,
          7459.0,
          7500.0,
          7540.0,
          7580.0,
          7621.0,
          7661.0,
          7701.0,
          7742.0,
          7782.0,
          7822.0,
          7863.0,
          7903.0,
          7943.0,
          7984.0,
          8024.0,
          8064.0,
          8105.0,
          8145.0,
          8185.0,
          8225.0,
          8266.0,
          8306.0,
          8346.0,
          8387.0,
          8427.0,
          8467.0,
          8508.0,
          8548.0,
          8588.0,
          8629.0,
          8669.0,
          8709.0,
          8750.0,
          8790.0,
          8830.0,
          8871.0,
          8911.0,
          8951.0,
          8992.0,
          9032.0,
          9072.0,
          9112.0,
          9153.0,
          9193.0,
          9233.0,
          9274.0,
          9314.0,
          9354.0,
          9395.0,
          9435.0,
          9475.0,
          9516.0,
          9556.0,
          9596.0,
          9637.0,
          9677.0,
          9717.0,
          9758.0,
          9798.0,
          9838.0,
          9879.0,
          9919.0,
          9959.0,
          10000.0
        ]
      ) AS bucket
    WHERE
      val >= CAST(bucket AS INT64)
  )
);

CREATE TEMP FUNCTION udf_boolean_buckets(
  scalar_aggs ARRAY<
    STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, value FLOAT64>
  >
)
RETURNS ARRAY<
  STRUCT<metric STRING, metric_type STRING, key STRING, agg_type STRING, bucket STRING>
> AS (
  (
    WITH boolean_columns AS (
      SELECT
        metric,
        metric_type,
        key,
        agg_type,
        CASE
          agg_type
        WHEN
          'true'
        THEN
          value
        ELSE
          0
        END
        AS bool_true,
        CASE
          agg_type
        WHEN
          'false'
        THEN
          value
        ELSE
          0
        END
        AS bool_false
      FROM
        UNNEST(scalar_aggs)
      WHERE
        metric_type IN ("boolean")
    ),
    summed_bools AS (
      SELECT
        metric,
        metric_type,
        key,
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
        WHEN
          bool_true > 0
          AND bool_false > 0
        THEN
          "sometimes"
        WHEN
          bool_true > 0
          AND bool_false = 0
        THEN
          "always"
        WHEN
          bool_true = 0
          AND bool_false > 0
        THEN
          "never"
        END
        AS bucket
      FROM
        summed_bools
      WHERE
        bool_true > 0
        OR bool_false > 0
    )
    SELECT
      ARRAY_AGG((metric, metric_type, key, agg_type, bucket))
    FROM
      booleans
  )
);

WITH bucketed_booleans AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    udf_boolean_buckets(scalar_aggregates) AS scalar_aggregates
  FROM
    glam_etl.org_mozilla_fenix__clients_scalar_aggregates_v1
),
bucketed_scalars AS (
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
    SAFE_CAST(udf_bucket(SAFE_CAST(value AS FLOAT64)) AS STRING) AS bucket
  FROM
    glam_etl.org_mozilla_fenix__clients_scalar_aggregates_v1
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE
    metric_type IN ("counter", "quantity", "labeled_counter")
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
)
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
  bucket,
  COUNT(*) AS count
FROM
  booleans_and_scalars
GROUP BY
  ping_type,
  os,
  app_version,
  app_build_id,
  channel,
  metric,
  metric_type,
  key,
  client_agg_type,
  bucket
