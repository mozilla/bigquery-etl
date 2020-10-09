/*
Normalize a histogram. Set sum to 1, and normalize to 1
the histogram bucket counts.
*/
CREATE OR REPLACE FUNCTION hist.normalize(
  histogram STRUCT<
    bucket_count INT64,
    `sum` INT64,
    histogram_type INT64,
    `range` ARRAY<INT64>,
    `values` ARRAY<STRUCT<key INT64, value INT64>>
  >
)
RETURNS STRUCT<
  bucket_count INT64,
  `sum` INT64,
  histogram_type INT64,
  `range` ARRAY<INT64>,
  `values` ARRAY<STRUCT<key INT64, value FLOAT64>>
> AS (
  STRUCT(
    histogram.bucket_count AS bucket_count,
    1 AS `sum`,
    histogram.histogram_type AS histogram_type,
    histogram.`range` AS `range`,
    ARRAY(
      SELECT AS STRUCT
        key,
        SAFE_DIVIDE(value, SUM(value) OVER ())
      FROM
        UNNEST(histogram.`values`)
    ) AS `values`
  )
);

-- Test
SELECT
  assert.histogram_equals(
    STRUCT(
      10 AS bucket_count,
      1 AS `sum`,
      1 AS histogram_type,
      [0, 10] AS `range`,
      [STRUCT(1 AS key, 0.5 AS value), STRUCT(2 AS key, 0.5 AS value)] AS `values`
    ),
    hist.normalize(
      STRUCT(
        10 AS bucket_count,
        6 AS `sum`,
        1 AS histogram_type,
        [0, 10] AS `range`,
        [STRUCT(1 AS key, 3 AS value), STRUCT(2 AS key, 3 AS value)] AS `values`
      )
    )
  );
