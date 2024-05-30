-- udf_js.glean_percentile
CREATE OR REPLACE FUNCTION glam.percentile(
  pct FLOAT64,
  histogram ARRAY<STRUCT<key STRING, value FLOAT64>>,
  type STRING
)
RETURNS FLOAT64
AS (
  (
    WITH
      check AS (
        SELECT IF(pct >= 0 AND pct <= 100, TRUE, ERROR('percentile must be a value between 0 and 100')) pct_ok
      ),
      keyed_cum_sum AS (
        SELECT
          key,
          SUM(value) OVER (ORDER BY key) / SUM(value) OVER () AS cum_sum
        FROM
          UNNEST(histogram)
      )
    SELECT
      CAST(key AS FLOAT64)
    FROM
      keyed_cum_sum,
      check
    WHERE
      check.pct_ok
      AND cum_sum >= pct / 100
    ORDER BY
      cum_sum
    LIMIT 1
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
  );

#xfail
SELECT
  glam.percentile(
    101.0,
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1), ("2", 2), ("3", 1)],
    "timing_distribution"
  );
