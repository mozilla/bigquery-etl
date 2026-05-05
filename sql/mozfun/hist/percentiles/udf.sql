/*
Given histogram and list of percentiles, calculate what those percentiles are for the histogram.

If the histogram is empty, returns NULL.
*/
CREATE OR REPLACE FUNCTION hist.percentiles(histogram ANY TYPE, percentiles ARRAY<FLOAT64>)
RETURNS ARRAY<STRUCT<percentile FLOAT64, value INT64>> AS (
  ARRAY(
    SELECT AS STRUCT
      percentile,
      MIN(key) AS value
    FROM
      (
        SELECT AS STRUCT
          key,
          SAFE_DIVIDE(
            1.0 * SUM(value) OVER (ORDER BY key),
            SUM(value) OVER ()
          ) AS cumulative_fraction
        FROM
          UNNEST(histogram.VALUES)
      )
    INNER JOIN
      UNNEST(percentiles) AS percentile
      ON cumulative_fraction >= percentile
    GROUP BY
      percentile
    ORDER BY
      percentile ASC
  )
);

SELECT
  assert.array_equals(
    [10, 50, 90],
    ARRAY(
      SELECT
        value
      FROM
        UNNEST(
          hist.percentiles(
            STRUCT(
              ARRAY(
                SELECT
                  STRUCT(v AS key, 1 AS value)
                FROM
                  UNNEST(GENERATE_ARRAY(1, 100)) AS v
              ) AS values
            ),
            [.1, .5, .9]
          )
        )
    )
  ),
  assert.array_equals(
    [100],
    ARRAY(
      SELECT
        value
      FROM
        UNNEST(
          hist.percentiles(
            STRUCT([STRUCT(99 AS key, 100 AS value), STRUCT(100, 101)] AS values),
            [.5]
          )
        )
    )
  ),
  assert.array_equals(
    CAST(NULL AS ARRAY<STRUCT<percentile FLOAT64, value INT64>>),
    hist.percentiles(STRUCT([STRUCT(0 AS key, 0 AS value)] AS values), [.2, .5])
  );
