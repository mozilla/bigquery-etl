-- udf_js.glean_percentile
CREATE OR REPLACE FUNCTION glam.percentile(
  pct FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>,
  type STRING
)
RETURNS FLOAT64 AS (
  (
    WITH check AS (
      SELECT
        IF(
          pct >= 0
          AND pct <= 100,
          TRUE,
          ERROR('percentile must be a value between 0 and 100')
        ) pct_ok,
        SUM(value) AS total_value
      FROM
        UNNEST(histogram)
    ),
    keyed_cum_sum AS (
      SELECT
        key,
        IF(
          total_value = 0,
          0,
          SUM(value) OVER (ORDER BY CAST(key AS FLOAT64)) / SUM(value) OVER ()
        ) cum_sum
      FROM
        UNNEST(histogram),
        check
    ),
    max_bucket AS (
      SELECT
        MAX(CAST(key AS FLOAT64)) AS bucket
      FROM
        UNNEST(histogram)
    )
    SELECT
      IF(total_value = 0, max_bucket.bucket, CAST(key AS FLOAT64))
    FROM
      keyed_cum_sum,
      check,
      max_bucket
    WHERE
      check.pct_ok
      AND (total_value = 0 OR cum_sum >= pct / 100)
    ORDER BY
      cum_sum
    LIMIT
      1
  )
);

SELECT
  assert.equals(
    2,
    glam.percentile(
      50.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
      "timing_distribution"
    )
  ),
  assert.equals(
    3,
    glam.percentile(
      100.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
      "timing_distribution"
    )
  ),
  assert.equals(
    0,
    glam.percentile(
      0.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
      "timing_distribution"
    )
  ),
  assert.equals(
    2,
    glam.percentile(
      2.0,
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("10", 10), ("11", 100)],
      "timing_distribution"
    )
  );

#xfail
SELECT
  glam.percentile(
    101.0,
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
    "timing_distribution"
  );
