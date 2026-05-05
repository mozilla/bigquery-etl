/*
Given an array of scalar metric data that might have
duplicate values for a metric, merge them into one value.
*/
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

-- Tests
WITH user_scalar_data AS (
  SELECT
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
      ("name1", "type", "", "parent", "max", 5.0),
      ("name1", "type", "", "parent", "max", 15.0),
      ("name1", "type", "", "parent", "max", 20.0),
      ("name1", "type", "", "parent", "min", 5.0),
      ("name1", "type", "", "parent", "min", 15.0),
      ("name1", "type", "", "parent", "min", 20.0),
      ("name1", "type", "", "parent", "count", 5.0),
      ("name1", "type", "", "parent", "count", 15.0),
      ("name1", "type", "", "parent", "count", 20.0),
      ("name1", "type", "", "parent", "sum", 105.0),
      ("name1", "type", "", "parent", "sum", 15.0),
      ("name1", "type", "", "parent", "sum", 20.0)
    ] AS scalar_aggs
),
applied_function AS (
  SELECT
    udf.merge_scalar_user_data(scalar_aggs) AS scalar_aggs
  FROM
    user_scalar_data
),
expected AS (
  SELECT
    [
      STRUCT(
        "name1" AS metric,
        "type" AS metric_type,
        "" AS key,
        "parent" AS process,
        "max" AS agg_type,
        20 AS value
      ),
      STRUCT(
        "name1" AS metric,
        "type" AS metric_type,
        "" AS key,
        "parent" AS process,
        "min" AS agg_type,
        5 AS value
      ),
      STRUCT(
        "name1" AS metric,
        "type" AS metric_type,
        "" AS key,
        "parent" AS process,
        "count" AS agg_type,
        40 AS value
      ),
      STRUCT(
        "name1" AS metric,
        "type" AS metric_type,
        "" AS key,
        "parent" AS process,
        "sum" AS agg_type,
        140 AS value
      ),
      STRUCT(
        "name1" AS metric,
        "type" AS metric_type,
        "" AS key,
        "parent" AS process,
        "avg" AS agg_type,
        3.5 AS value
      )
    ] AS arr
),
merged_expected_actual AS (
  SELECT
    ANY_VALUE(expected) AS expected,
    ANY_VALUE(actual) AS actual
  FROM
    (
      SELECT
        arr AS expected,
        NULL AS actual
      FROM
        expected
      UNION ALL
      SELECT
        NULL AS expected,
        scalar_aggs AS actual
      FROM
        applied_function
    )
)
SELECT
  mozfun.assert.array_equals(expected, actual)
FROM
  merged_expected_actual
