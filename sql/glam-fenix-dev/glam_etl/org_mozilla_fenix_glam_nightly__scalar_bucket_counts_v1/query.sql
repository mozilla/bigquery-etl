-- query for org_mozilla_fenix_glam_nightly__scalar_bucket_counts_v1;
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
      ARRAY_AGG((metric, metric_type, key, agg_type, bucket))
    FROM
      booleans
  )
);

WITH build_ids AS (
  SELECT
    app_build_id,
    channel,
  FROM
    glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1
  GROUP BY
    app_build_id,
    channel
  HAVING
    COUNT(DISTINCT client_id) > 800
),
valid_clients_scalar_aggregates AS (
  SELECT
    *
  FROM
    glam_etl.org_mozilla_fenix_glam_nightly__clients_scalar_aggregates_v1
  INNER JOIN
    build_ids
    USING (app_build_id, channel)
),
bucketed_booleans AS (
  SELECT
    client_id,
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    NULL AS range_min,
    NULL AS range_max,
    NULL AS bucket_count,
    udf_boolean_buckets(scalar_aggregates) AS scalar_aggregates,
  FROM
    valid_clients_scalar_aggregates
),
log_min_max AS (
  SELECT
    metric,
    key,
    LOG(IF(MIN(value) <= 0, 1, MIN(value)), 2) AS range_min,
    LOG(IF(MAX(value) <= 0, 1, MAX(value)), 2) AS range_max,
    100 AS bucket_count
  FROM
    valid_clients_scalar_aggregates
  CROSS JOIN
    UNNEST(scalar_aggregates)
  WHERE
    metric_type <> "boolean"
  GROUP BY
    metric,
    key
),
buckets_by_metric AS (
  SELECT
    *,
    ARRAY(
      SELECT
        FORMAT("%.*f", 2, bucket)
      FROM
        UNNEST(
          mozfun.glam.histogram_generate_scalar_buckets(range_min, range_max, bucket_count)
        ) AS bucket
    ) AS buckets
  FROM
    log_min_max
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
    range_min,
    range_max,
    bucket_count,
    -- Keep two decimal places before converting bucket to a string
    SAFE_CAST(
      FORMAT("%.*f", 2, mozfun.glam.histogram_bucket_from_value(buckets, value) + 0.0001) AS STRING
    ) AS bucket
  FROM
    valid_clients_scalar_aggregates
  CROSS JOIN
    UNNEST(scalar_aggregates)
  LEFT JOIN
    buckets_by_metric
    USING (metric, key)
  WHERE
    metric_type IN ("counter", "quantity", "labeled_counter", "timespan")
),
booleans_and_scalars AS (
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
    range_min,
    range_max,
    bucket_count,
    bucket
  FROM
    bucketed_booleans
  CROSS JOIN
    UNNEST(scalar_aggregates)
  UNION ALL
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
    range_min,
    range_max,
    bucket_count,
    bucket
  FROM
    bucketed_scalars
),
booleans_and_scalars_grouped AS (
  SELECT
    ping_type,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    agg_type,
    range_min,
    range_max,
    bucket_count,
    bucket,
    -- Aggregate client ids into HLL sketch before cross joining with
    -- attribute combinations to make the final aggregation cheaper
    HLL_COUNT.INIT(client_id, 24) AS clients_hll,
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
    agg_type,
    range_min,
    range_max,
    bucket_count,
    bucket
),
-- Cross join with the attribute combinations to reduce the query complexity
-- with respect to the number of operations. A table with n rows cross joined
-- with a combination of m attributes will generate a new table with n*m rows.
-- The glob ("*") symbol can be understood as selecting all of values belonging
-- to that group.
static_combos AS (
  SELECT
    combos.*
  FROM
    UNNEST(
      ARRAY<STRUCT<ping_type STRING, os STRING, app_build_id STRING>>[
        (NULL, NULL, NULL),
        (NULL, NULL, "*"),
        (NULL, "*", NULL),
        ("*", NULL, NULL),
        (NULL, "*", "*"),
        ("*", NULL, "*"),
        ("*", "*", NULL),
        ("*", "*", "*")
      ]
    ) AS combos
),
all_combos AS (
  SELECT
    table.* EXCEPT (ping_type, os, app_build_id),
    COALESCE(combo.ping_type, table.ping_type) AS ping_type,
    COALESCE(combo.os, table.os) AS os,
    COALESCE(combo.app_build_id, table.app_build_id) AS app_build_id
  FROM
    booleans_and_scalars_grouped table
  CROSS JOIN
    static_combos combo
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
  range_min,
  range_max,
  bucket_count,
  bucket,
  HLL_COUNT.MERGE(clients_hll) AS count
FROM
  all_combos
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
  range_min,
  range_max,
  bucket_count,
  bucket
