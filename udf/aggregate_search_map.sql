CREATE TEMP FUNCTION
  udf_aggregate_search_map(engine_searches_list ANY TYPE)
    AS (
      ARRAY(
        SELECT AS STRUCT
          v.key,
          STRUCT(
            udf_zeroed_array_with_last_val(SUM(v.value.total_searches)) AS total_searches,
            udf_zeroed_array_with_last_val(SUM(v.value.tagged_searches)) AS tagged_searches,
            udf_zeroed_array_with_last_val(SUM(v.value.search_with_ads)) AS search_with_ads,
            udf_zeroed_array_with_last_val(SUM(v.value.ad_click)) AS ad_click
          ) AS value
        FROM
          UNNEST(engine_searches_list) AS v
        GROUP BY
          v.key
    )
);

WITH output AS (
  SELECT udf_aggregate_search_map(ARRAY [ STRUCT("google" AS key, STRUCT(5 AS total_searches, 0 AS tagged_searches, 0 AS search_with_ads, 0 AS ad_click) AS value),
                                     STRUCT("google" AS key, STRUCT(5 AS total_searches, 5 AS tagged_searches, 0 AS search_with_ads, 0 AS ad_click) AS value) ]) AS res
)

SELECT
  assert_equals("google", res[OFFSET(0)].key),
  assert_array_equals(udf_zeroed_array_with_last_val(10), res[OFFSET(0)].value.total_searches),
  assert_array_equals(udf_zeroed_array_with_last_val(5), res[OFFSET(0)].value.tagged_searches),
  assert_array_equals(udf_zeroed_array_with_last_val(0), res[OFFSET(0)].value.search_with_ads),
  assert_array_equals(udf_zeroed_array_with_last_val(0), res[OFFSET(0)].value.ad_click),
  assert_equals(array_length(res), 1)
FROM
  output;
