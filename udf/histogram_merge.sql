/*
*/
CREATE OR REPLACE FUNCTION udf.histogram_merge(
  histogram_list ARRAY<
    STRUCT<
      bucket_count INT64,
      `sum` INT64,
      histogram_type INT64,
      `range` ARRAY<INT64>,
      VALUES
        ARRAY<STRUCT<key INT64, value INT64>>
    >
  >
)
RETURNS STRUCT<
  bucket_count INT64,
  `sum` INT64,
  histogram_type INT64,
  `range` ARRAY<INT64>,
  VALUES
    ARRAY<STRUCT<key INT64, value INT64>>
> AS (
  STRUCT(
    udf.mode_last(ARRAY(SELECT bucket_count FROM UNNEST(histogram_list))) AS bucket_count,
    (SELECT SUM(`sum`) FROM UNNEST(histogram_list)) AS `sum`,
    udf.mode_last(ARRAY(SELECT histogram_type FROM UNNEST(histogram_list))) AS histogram_type,
    [
      udf.mode_last(ARRAY(SELECT `range`[SAFE_OFFSET(0)] FROM UNNEST(histogram_list))),
      udf.mode_last(ARRAY(SELECT `range`[SAFE_OFFSET(1)] FROM UNNEST(histogram_list)))
    ] AS `range`,
    ARRAY(
      SELECT AS STRUCT
        key,
        SUM(value)
      FROM
        UNNEST(histogram_list) AS histogram,
        UNNEST(VALUES)
      GROUP BY
        key
    ) AS VALUES
  )
);

-- Test
WITH histograms AS (
  SELECT
    STRUCT(
      5 AS bucket_count,
      20 AS `sum`,
      1 AS histogram_type,
      [0, 100] AS `range`,
      [STRUCT(0 AS key, 0 AS value), STRUCT(20 AS key, 1 AS value)] AS values
    ) AS h,
  UNION ALL
  SELECT
    STRUCT(
      5 AS bucket_count,
      40 AS `sum`,
      1 AS histogram_type,
      [0, 100] AS `range`,
      [STRUCT(0 AS key, 0 AS value), STRUCT(40 AS key, 1 AS value)] AS values
    )
),
merged AS (
  SELECT
    udf.histogram_merge(ARRAY_AGG(h)) AS h
  FROM
    histograms
)
SELECT
  assert_equals(5, h.bucket_count),
  assert_equals(60, h.`sum`),
  assert_equals(1, h.histogram_type),
  assert_array_equals([0, 100], h.`range`),
  assert_equals(0, udf.get_key(h.values, 0)),
  assert_equals(1, udf.get_key(h.values, 20)),
  assert_equals(1, udf.get_key(h.values, 40)),
FROM
  merged;
