CREATE OR REPLACE FUNCTION udf.sum_array_struct(
  sum_array ARRAY<STRUCT<key STRING, value INT64>>
) AS (
  (SELECT AS STRUCT COALESCE(SUM(value), 0) AS array_metric FROM UNNEST(sum_array))
);

SELECT
  assert_equals(
    STRUCT(6 AS array_metric,),
    udf.sum_array_struct(
      [
        STRUCT('foo' AS key, 3 AS value),
        STRUCT('foo' AS key, 2 AS value),
        STRUCT('foo' AS key, 1 AS value),
        STRUCT('foo' AS key, NULL AS value)
      ]
    )
  ),
  assert_equals(STRUCT(0 AS array_metric), udf.sum_array_struct([]));
