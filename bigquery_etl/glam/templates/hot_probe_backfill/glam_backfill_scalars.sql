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
      ARRAY_AGG((metric, metric_type, key, process, agg_type, bucket))
    FROM
      booleans
  )
);
WITH per_build_client_day AS (
  SELECT
        DATE(submission_timestamp) AS submission_date,
        client_id,
        normalized_os AS os,
        CAST(SPLIT(application.version, '.')[OFFSET(0)] AS INT64) AS app_version,
        application.build_id AS app_build_id,
        normalized_channel as channel,
        -- If source table is main_1pct_v1 it's always sampled
        --normalized_os = 'Windows'
        --AND normalized_channel = 'release' AS sampled,
        TRUE AS sampled,
        ARRAY<STRUCT<name STRING, process STRING,
        {% if is_keyed %}
            value ARRAY<STRUCT<key STRING, value INT64>>
        {% else %}
            value INT64
        {% endif %}
        >>[
       (
        '{{ metric }}',
        '{{ process }}',
        {{ probe_location }}
      )
    ] AS metrics
   FROM   `{{ project }}.{{ source_table }}`

    WHERE DATE(submission_timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL {{ days }} DAY)
    AND DATE(submission_timestamp) <= CURRENT_DATE
),
flattened_metrics AS (
  SELECT
    client_id,
    submission_date,
    os,
    app_version,
    app_build_id,
    channel,
    metrics.name AS metric,
    metrics.process AS process,
    {% if is_keyed %}
        value.key AS key,
        value.value AS value,
    {% else %}
        '' AS key,
        value,
    {% endif %}
    sampled
  FROM
    per_build_client_day
  CROSS JOIN
    UNNEST(metrics) AS metrics
    {% if is_keyed %}
    ,
    CROSS JOIN
      UNNEST(value) AS value
    {% endif %}
),
aggregated AS (
  SELECT
    submission_date,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    process,
    key,
    MAX(value) AS max,
    MIN(value) AS min,
    AVG(value) AS avg,
    SUM(value) AS sum,
    IF(MIN(value) IS NULL, NULL, COUNT(*)) AS count,
    sampled
  FROM
    flattened_metrics
  GROUP BY
    submission_date,
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    process,
    key,
    sampled
),
final_aggregates AS (SELECT
  client_id,
  submission_date,
  os,
  app_version,
  app_build_id,
  channel,
  sampled,
  ARRAY_CONCAT_AGG(
    ARRAY<
      STRUCT<
        metric STRING,
        metric_type STRING,
        key STRING,
        process STRING,
        agg_type STRING,
        value FLOAT64
      >
    >[
      (metric, '{{ metric_type }}', key, process, 'max', max),
      (metric, '{{ metric_type }}', key, process, 'min', min),
      (metric, '{{ metric_type }}', key, process, 'avg', avg),
      (metric, '{{ metric_type }}', key, process, 'sum', sum),
      (metric, '{{ metric_type }}', key, process, 'count', count)
    ]
  ) AS scalar_aggregates
FROM
  aggregated
GROUP BY
  client_id,
  submission_date,
  os,
  app_version,
  app_build_id,
  channel,
  sampled
),
log_min_max AS (
  SELECT
    metric,
    key,
    LOG(IF(MIN(value) <= 0, 1, MIN(value)), 2) log_min,
    LOG(IF(MAX(value) <= 0, 1, MAX(value)), 2) log_max
  FROM
    final_aggregates
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
    final_aggregates flat_table
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
    `moz-fx-data-shared-prod`.udf.merge_scalar_user_data(ARRAY_CONCAT_AGG(scalar_aggregates)) AS scalar_aggregates
  FROM
    all_combos
  LEFT JOIN
    `moz-fx-data-shared-prod.telemetry_derived.latest_versions`
  USING(channel)
  WHERE
    app_version >= (latest_version - 10)
  GROUP BY
    client_id,
    os,
    app_version,
    app_build_id,
    channel
),
bucketed_booleans AS (
  SELECT
    client_id,
    os,
    app_version,
    app_build_id,
    channel,
    user_count,
    -- If source table is main_1pct_v1 it's always sampled
    -- os = 'Windows'
    -- AND channel = 'release' AS sampled,
    TRUE AS sampled,
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
    -- If source table is main_1pct_v1 it's always sampled
    -- os = 'Windows'
    -- AND channel = 'release' AS sampled,
    TRUE AS sampled,
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
  USING
    (metric, key)
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
    booleans_and_scalars
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
probe_counts AS (SELECT
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
  WHEN
    metric_type = 'scalar'
    OR metric_type = 'keyed-scalar'
  THEN
    mozfun.glam.histogram_fill_buckets(
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, user_count)),
      ANY_VALUE(buckets)
    )
  WHEN
    metric_type = 'boolean'
    OR metric_type = 'keyed-scalar-boolean'
  THEN
    mozfun.glam.histogram_fill_buckets(
      ARRAY_AGG(STRUCT<key STRING, value FLOAT64>(bucket, user_count)),
      ['always', 'never', 'sometimes']
    )
  END
  AS histogram
FROM
  clients_scalar_bucket_counts
LEFT JOIN
  buckets_by_metric
USING
  (metric, key)
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
),
percentiles AS (
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
    agg_type AS client_agg_type,
    'percentiles' AS agg_type,
    SUM(user_count) AS total_users,
    APPROX_QUANTILES(value, 1000)  AS percents
  FROM
    user_aggregates
  CROSS JOIN UNNEST(scalar_aggregates)
  GROUP BY
    os,
    app_version,
    app_build_id,
    channel,
    metric,
    metric_type,
    key,
    process,
    client_agg_type
),

finalextract AS
    (
         SELECT  pc.app_version,
    pc.os,
    pc.app_build_id,
    pc.process,
    pc.metric,
    pc.channel,
    pc.key,
    pc.client_agg_type,
    pc.metric_type,
    pc.total_users,
    mozfun.glam.histogram_cast_json(histogram) AS histogram,
    mozfun.glam.map_from_array_offsets_precise(
  [0.1, 1.0, 5.0, 25.0, 50.0, 75.0, 95.0, 99.0, 99.9],
  percents) AS percentiles
         FROM   probe_counts pc
    INNER JOIN percentiles per ON pc.app_version = per.app_version
    AND pc.os = per.os
    AND pc.channel = per.channel
    AND pc.metric = per.metric
    AND pc.metric_type = per.metric_type
    AND pc.key = per.key
    AND pc.app_build_id = per.app_build_id
    AND pc.process = per.process
    AND pc.client_agg_type = per.client_agg_type
    )
  SELECT * replace(mozfun.glam.histogram_cast_json(percentiles) AS percentiles)
  FROM   finalextract

