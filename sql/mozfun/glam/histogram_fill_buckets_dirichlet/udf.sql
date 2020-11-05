-- udf_fill_buckets
CREATE OR REPLACE FUNCTION glam.histogram_fill_buckets_dirichlet(
  input_map ARRAY<STRUCT<key STRING, value FLOAT64>>,
  buckets ARRAY<STRING>,
  total_users INT64
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Given a MAP `input_map`, fill in any missing keys with value `0.0`
  ARRAY(
    SELECT AS STRUCT
      key,
      -- Dirichlet distribution density for each bucket in a histogram.
      -- Given {k1: p1,k2:p2} where p’s are proportions(and p1, p2 sum to 1)
      -- return {k1: (P1+1/K) / (nreporting+1), k2:(P2+1/K) / (nreporting+1)}.
      -- https://docs.google.com/document/d/1ipy1oFIKDvHr3R6Ku0goRjS11R1ZH1z2gygOGkSdqUg
      SAFE_DIVIDE(
        COALESCE(e.value, 0.0) + SAFE_DIVIDE(1, ARRAY_LENGTH(buckets)),
        total_users + 1
      ) AS value
    FROM
      UNNEST(buckets) AS key
    LEFT JOIN
      UNNEST(input_map) AS e
    ON
      key = e.key
    ORDER BY
      SAFE_CAST(key AS FLOAT64)
  )
);

SELECT
  -- fill in 1 with a value of 0
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[
      ("0", (1 + (1 / 3)) / 3),
      ("1", 1 / 9),
      ("2", (2 + (1 / 3)) / 3)
    ],
    glam.histogram_fill_buckets_dirichlet(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("2", 2.0)],
      ["0", "1", "2"],
      2
    )
  ),
  -- only keep values in specified in buckets
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", (1 + 1) / 3)],
    glam.histogram_fill_buckets_dirichlet(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 1.0), ("2", 1.0)],
      ["0"],
      2
    )
  ),
  -- keys may not non-integer values, so ordering is not well defined for strings
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[
      ("foo", (1 + (1 / 2)) / 3),
      ("bar", (1 + (1 / 2)) / 3)
    ],
    glam.histogram_fill_buckets_dirichlet(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("foo", 1.0), ("bar", 1.0)],
      ["foo", "bar"],
      2
    )
  ),
  -- but ordering is guaranteed for integers/floats
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("2", (1 + (1 / 2)) / 3), ("11", (1 + (1 / 2)) / 3)],
    glam.histogram_fill_buckets_dirichlet(
      ARRAY<STRUCT<key STRING, value FLOAT64>>[("11", 1.0), ("2", 1.0)],
      ["11", "2"],
      2
    )
  )
