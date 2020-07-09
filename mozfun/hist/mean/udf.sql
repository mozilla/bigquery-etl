/*

Given histogram h, return floor(mean) of the measurements in the bucket.
That is, the histogram sum divided by the number of measurements taken.

https://github.com/mozilla/telemetry-batch-view/blob/ea0733c/src/main/scala/com/mozilla/telemetry/utils/MainPing.scala#L292-L307

*/
CREATE OR REPLACE FUNCTION mozfun.hist.mean(histogram ANY TYPE) AS (
  CASE
  WHEN
    histogram.sum < 0
  THEN
    NULL
  WHEN
    histogram.sum = 0
  THEN
    0
  ELSE
    SAFE_CAST(
      TRUNC(
        histogram.sum / (SELECT SUM(value) FROM UNNEST(histogram.values) WHERE value > 0)
      ) AS INT64
    )
  END
);

SELECT
  assert_equals(
    30798,
    mozfun.hist.mean(STRUCT(30798 AS sum, [STRUCT(0 AS value), STRUCT(1), STRUCT(0)] AS values))
  ),
  assert_equals(
    15399,
    mozfun.hist.mean(STRUCT(30798 AS sum, [STRUCT(0 AS value), STRUCT(2), STRUCT(0)] AS values))
  ),
  assert_equals(
    10266,
    mozfun.hist.mean(STRUCT(30798 AS sum, [STRUCT(1 AS value), STRUCT(2), STRUCT(0)] AS values))
  ),
  assert_equals(
    7699,
    mozfun.hist.mean(STRUCT(30798 AS sum, [STRUCT(1 AS value), STRUCT(2), STRUCT(1)] AS values))
  ),
  assert_equals(0, mozfun.hist.mean(STRUCT(0 AS sum, ARRAY<STRUCT<value INT64>>[] AS values))),
  assert_null(mozfun.hist.mean(STRUCT(10 AS sum, [STRUCT(0 AS value)] AS values))),
  assert_null(mozfun.hist.mean(CAST(NULL AS STRUCT<sum INT64, VALUES ARRAY<STRUCT<value INT64>>>)))
