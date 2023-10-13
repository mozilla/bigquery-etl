/*
Returns an aggregated map with all the keys and the first corresponding value
from the given maps
*/
CREATE OR REPLACE FUNCTION udf.aggregate_map_first(maps ANY TYPE) AS (
  STRUCT(
    ARRAY(
      SELECT AS STRUCT
        * EXCEPT (_n)
      FROM
        (
          SELECT
            * EXCEPT (value),
            FIRST_VALUE(
              value IGNORE NULLS
            ) --
            OVER (
              PARTITION BY
                key
              ORDER BY
                _n
              ROWS BETWEEN
                UNBOUNDED PRECEDING
                AND UNBOUNDED FOLLOWING
            ) AS value
          FROM
            (
              SELECT
                ROW_NUMBER() OVER (PARTITION BY key) AS _n,
                key,
                value
              FROM
                UNNEST(maps),
                UNNEST(key_value)
            )
        )
      WHERE
        _n = 1
    ) AS key_value
  )
);

-- Test
SELECT
  mozfun.assert.array_equals(
    [STRUCT('A' AS key, '2' AS value), STRUCT('B' AS key, '1' AS value)],
    udf.aggregate_map_first(
      [
        STRUCT([STRUCT('B' AS key, '1' AS value)] AS key_value),
        STRUCT([STRUCT('A' AS key, '2' AS value), STRUCT('B' AS key, '3' AS value)] AS key_value),
        STRUCT([STRUCT('A' AS key, '4' AS value)] AS key_value),
        STRUCT([STRUCT('B' AS key, '5' AS value)] AS key_value)
      ]
    ).key_value
  )
