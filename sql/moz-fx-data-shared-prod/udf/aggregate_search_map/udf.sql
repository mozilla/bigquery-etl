/*
Aggregates the total counts of the given search counters
*/
CREATE OR REPLACE FUNCTION udf.aggregate_search_map(engine_searches_list ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      v.key,
      STRUCT(
        udf.array_11_zeroes_then(SUM(v.value.total_searches)) AS total_searches,
        udf.array_11_zeroes_then(SUM(v.value.tagged_searches)) AS tagged_searches,
        udf.array_11_zeroes_then(SUM(v.value.search_with_ads)) AS search_with_ads,
        udf.array_11_zeroes_then(SUM(v.value.ad_click)) AS ad_click
      ) AS value
    FROM
      UNNEST(engine_searches_list) AS v
    GROUP BY
      v.key
  )
);

-- Tests
WITH output AS (
  SELECT
    udf.aggregate_search_map(
      ARRAY[
        STRUCT(
          "google" AS key,
          STRUCT(
            5 AS total_searches,
            0 AS tagged_searches,
            0 AS search_with_ads,
            0 AS ad_click
          ) AS value
        ),
        STRUCT(
          "google" AS key,
          STRUCT(
            5 AS total_searches,
            5 AS tagged_searches,
            0 AS search_with_ads,
            0 AS ad_click
          ) AS value
        )
      ]
    ) AS res
)
SELECT
  mozfun.assert.equals("google", res[OFFSET(0)].key),
  mozfun.assert.array_equals(udf.array_11_zeroes_then(10), res[OFFSET(0)].value.total_searches),
  mozfun.assert.array_equals(udf.array_11_zeroes_then(5), res[OFFSET(0)].value.tagged_searches),
  mozfun.assert.array_equals(udf.array_11_zeroes_then(0), res[OFFSET(0)].value.search_with_ads),
  mozfun.assert.array_equals(udf.array_11_zeroes_then(0), res[OFFSET(0)].value.ad_click),
  mozfun.assert.equals(ARRAY_LENGTH(res), 1)
FROM
  output;
