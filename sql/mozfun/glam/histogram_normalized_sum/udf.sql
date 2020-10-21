-- udf_normalized_sum
CREATE OR REPLACE FUNCTION glam.histogram_normalized_sum(
  arrs ARRAY<STRUCT<key STRING, value INT64>>,
  sampled BOOL
)
RETURNS ARRAY<STRUCT<key STRING, value FLOAT64>> AS (
  -- Input: one histogram for a single client.
  -- Returns the normalized sum of the input maps.
  -- It returns the total_count[k] / SUM(total_count)
  -- for each key k.
  (
    WITH total_counts AS (
      SELECT
        sum(a.value) AS total_count
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
    ),
    final_values AS (
      SELECT
        STRUCT<key STRING, value FLOAT64>(
          k,
          -- Weight probes from Windows release clients to account for 10% sampling
          COALESCE(SAFE_DIVIDE(1.0 * v, total_count), 0) * IF(sampled, 10, 1)
        ) AS record
      FROM
        summed_counts
      CROSS JOIN
        total_counts
    )
    SELECT
      ARRAY_AGG(record)
    FROM
      final_values
  )
);
