CREATE OR REPLACE FUNCTION udf.merge_scalar_user_data(
  aggs ARRAY<
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
    value FLOAT64
  >
> AS (
  (
    WITH unnested AS (
      SELECT
        *
      FROM
        UNNEST(aggs)
      WHERE
        agg_type != "avg"
    ),
    aggregated AS (
      SELECT
        metric,
        metric_type,
        key,
        process,
        agg_type,
        --format:off
        CASE agg_type
          WHEN 'max' THEN max(value)
          WHEN 'min' THEN min(value)
          WHEN 'count' THEN sum(value)
          WHEN 'sum' THEN sum(value)
          WHEN 'false' THEN sum(value)
          WHEN 'true' THEN sum(value)
        END AS value
        --format:on
      FROM
        unnested
      WHERE
        value IS NOT NULL
      GROUP BY
        metric,
        metric_type,
        key,
        process,
        agg_type
    ),
    scalar_count_and_sum AS (
      SELECT
        metric,
        metric_type,
        key,
        process,
        'avg' AS agg_type,
        --format:off
        CASE WHEN agg_type = 'count' THEN value ELSE 0 END AS count,
        CASE WHEN agg_type = 'sum' THEN value ELSE 0 END AS sum
        --format:on
      FROM
        aggregated
      WHERE
        agg_type IN ('sum', 'count')
    ),
    scalar_averages AS (
      SELECT
        * EXCEPT (count, sum),
        SUM(sum) / SUM(count) AS agg_value
      FROM
        scalar_count_and_sum
      GROUP BY
        metric,
        metric_type,
        key,
        process,
        agg_type
    ),
    merged_data AS (
      SELECT
        *
      FROM
        aggregated
      UNION ALL
      SELECT
        *
      FROM
        scalar_averages
    )
    SELECT
      ARRAY_AGG((metric, metric_type, key, process, agg_type, value))
    FROM
      merged_data
  )
);
