CREATE OR REPLACE FUNCTION glam.histogram_filter_high_values(
  aggs ARRAY<STRUCT<key STRING, value INT64>>
)
RETURNS ARRAY<STRUCT<key STRING, value INT64>> AS (
  ARRAY(
    SELECT AS STRUCT
      agg.key,
      SUM(agg.value) AS value
    FROM
      UNNEST(aggs) agg
    WHERE
      agg.value
      BETWEEN 0
      AND POW(2, 40)
    GROUP BY
      agg.key
  )
);

SELECT
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[('key1', 3), ('key2', 1099511627776)],
    glam.histogram_filter_high_values(
      [
        STRUCT(
          'key1' AS key,
          1 AS value
        ), -- The first two elements have the same key and should be summed
        STRUCT('key1' AS key, 2 AS value),
        STRUCT(
          'key2' AS key,
          1099511627776 AS value
        ), -- This is exactly 2^40 and should not be excluded
        STRUCT('key2' AS key, 1099511627777 AS value), -- This exceeds 2^40 and should be excluded
        STRUCT('key1' AS key, -5 AS value)  -- Should be excluded due to being negative
      ]
    )
  );

SELECT
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[],
    glam.histogram_filter_high_values([])
  );
