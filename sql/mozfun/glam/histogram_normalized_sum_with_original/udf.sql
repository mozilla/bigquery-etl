-- udf_normalized_sum_with_original
CREATE OR REPLACE FUNCTION glam.histogram_normalized_sum_with_original(
  arrs ARRAY<STRUCT<key STRING, value INT64>>,
  weight FLOAT64
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64, non_norm_value FLOAT64>> AS (
  -- Input: one histogram for a single client.
  -- Returns the normalized and the non-normalized sum of the input maps.
  -- It returns the total_count[k] / SUM(total_count) and total_count[k]
  -- for each key k.
  (
    WITH total_counts AS (
      SELECT
        SUM(a.value) AS total_count
      FROM
        UNNEST(arrs) AS a
    ),
    summed_counts AS (
      SELECT
        a.key AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(arrs) AS a
      GROUP BY
        a.key
    )
    SELECT
      ARRAY_AGG(
        STRUCT<key STRING, value FLOAT64, non_norm_value FLOAT64>(
          k,
          COALESCE(SAFE_DIVIDE(1.0 * v, total_count), 0) * weight,
          1.0 * v
        )
        ORDER BY
          SAFE_CAST(k AS INT64)
      )
    FROM
      summed_counts
    CROSS JOIN
      total_counts
  )
);

SELECT
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64, non_norm_value FLOAT64>>[
      ("0", 0.25, 1.0),
      ("1", 0.25, 1.0),
      ("2", 0.5, 2.0)
    ],
    glam.histogram_normalized_sum_with_original(
      ARRAY<STRUCT<key STRING, value INT64>>[("0", 1), ("1", 1), ("2", 2)],
      1.0
    )
  ),
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64, non_norm_value FLOAT64>>[
      ("0", 0.5, 1.0),
      ("1", 0.5, 1.0),
      ("2", 1.0, 2.0)
    ],
    glam.histogram_normalized_sum_with_original(
      ARRAY<STRUCT<key STRING, value INT64>>[("0", 1), ("1", 1), ("2", 2)],
      2.0
    )
  ),
  -- out of order keys
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64, non_norm_value FLOAT64>>[
      ("2", 0.5, 1.0),
      ("11", 0.5, 1.0)
    ],
    glam.histogram_normalized_sum_with_original(
      ARRAY<STRUCT<key STRING, value INT64>>[("11", 1), ("2", 1)],
      1
    )
  ),
   -- different inputs for same bucket
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64, non_norm_value FLOAT64>>[
      ("0", 0.5, 2.0),
      ("1", 0.25, 1.0),
      ("2", 0.25, 1.0)
    ],
    glam.histogram_normalized_sum_with_original(
      ARRAY<STRUCT<key STRING, value INT64>>[("0", 1), ("0", 1), ("1", 1), ("2", 1)],
      1
    )
  )
