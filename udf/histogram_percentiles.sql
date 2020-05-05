/*
Given histogram and list of percentiles, calculate what those percentiles are for the histogram.

If the histogram is empty, returns NULL.
*/
CREATE OR REPLACE FUNCTION udf.histogram_percentiles(histogram ANY TYPE, percentiles ARRAY<FLOAT64>)
RETURNS ARRAY<INT64> AS (
--   IF(
--     (SELECT SUM(value) FROM UNNEST(histogram.value)) = 0
  ARRAY(
    SELECT
      MIN(key)
    FROM
      (
        SELECT AS STRUCT
          key,
          SAFE_DIVIDE(
            1.0 * SUM(value) OVER (ORDER BY key),
            SUM(value) OVER ()
          ) AS cumulative_fraction
        FROM
          UNNEST(histogram.values)
      )
    INNER JOIN
      UNNEST(percentiles) AS percentile
    ON
      cumulative_fraction >= percentile
    GROUP BY
      percentile
    ORDER BY
      percentile ASC
  )
);

SELECT
  assert_array_equals(
    [10, 50, 90],
    udf.histogram_percentiles(
      STRUCT(
        ARRAY(
          SELECT
            STRUCT(v AS key, 1 AS value)
          FROM
            UNNEST(generate_array(1, 100)) AS v
        ) AS values
      ),
      [.1, .5, .9]
    )
  ),
  assert_array_equals(
    [100],
    udf.histogram_percentiles(
      STRUCT([STRUCT(99 AS key, 100 AS value), STRUCT(100, 101)] AS values),
      [.5]
    )
  ),
  assert_array_equals(
    CAST(NULL AS ARRAY<INT64>),
    udf.histogram_percentiles(STRUCT([STRUCT(0 AS key, 0 AS value)] AS values), [.2, .5])
  );
