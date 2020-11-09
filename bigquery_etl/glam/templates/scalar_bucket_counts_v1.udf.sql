CREATE TEMP FUNCTION udf_boolean_buckets(
  scalar_aggs ARRAY<
    STRUCT<
      {{ aggregate_attributes_type }},
      agg_type STRING,
      value FLOAT64
    >
  >
)
RETURNS ARRAY<
  STRUCT<
    {{ aggregate_attributes_type }},
    agg_type STRING,
    bucket STRING
  >
> AS (
  (
    WITH boolean_columns AS (
      SELECT
        {{ aggregate_attributes }},
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
        metric_type IN ({{ boolean_metric_types }})
    ),
    summed_bools AS (
      SELECT
        {{ aggregate_attributes }},
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
      ARRAY_AGG(({{ aggregate_attributes }}, agg_type, bucket))
    FROM
      booleans
  )
);
