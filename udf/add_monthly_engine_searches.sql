/*
*/

CREATE TEMP FUNCTION
  udf_add_monthly_engine_searches(prev STRUCT<total_searches ARRAY<INT64>, tagged_searches ARRAY<INT64>, search_with_ads ARRAY<INT64>, ad_click ARRAY<INT64>>,
                           curr STRUCT<total_searches ARRAY<INT64>, tagged_searches ARRAY<INT64>, search_with_ads ARRAY<INT64>, ad_click ARRAY<INT64>>,
                           submission_date DATE) AS (
  IF(EXTRACT(DAY FROM submission_date) = 1,
    STRUCT(
        udf_array_drop_first_and_append(prev.total_searches, curr.total_searches[OFFSET(11)]) AS total_searches,
        udf_array_drop_first_and_append(prev.tagged_searches, curr.tagged_searches[OFFSET(11)]) AS tagged_searches,
        udf_array_drop_first_and_append(prev.search_with_ads, curr.search_with_ads[OFFSET(11)]) AS search_with_ads,
        udf_array_drop_first_and_append(prev.ad_click, curr.ad_click[OFFSET(11)]) AS ad_click
    ),
    STRUCT(
        udf_vector_add(prev.total_searches, curr.total_searches) AS total_searches,
        udf_vector_add(prev.tagged_searches, curr.tagged_searches) AS tagged_searches,
        udf_vector_add(prev.search_with_ads, curr.search_with_ads) AS search_with_ads,
        udf_vector_add(prev.ad_click, curr.ad_click) AS ad_click
    )
));

-- Tests

WITH examples AS (
    SELECT
        STRUCT(generate_array(11, 0, -1) AS total_searches,
               generate_array(12, 1, -1) AS tagged_searches,
               udf_zeroed_array(12) AS search_with_ads,
               udf_zeroed_array(12) AS ad_click) AS prev,
        STRUCT(udf_array_drop_first_and_append(udf_zeroed_array(12),  5) AS total_searches,
               udf_array_drop_first_and_append(udf_zeroed_array(12), 10) AS tagged_searches,
               udf_array_drop_first_and_append(udf_zeroed_array(12), 15) AS search_with_ads,
               udf_array_drop_first_and_append(udf_zeroed_array(12), 20) AS ad_click) AS curr
), oct_first AS (
    SELECT
      udf_add_monthly_engine_searches(prev, curr, "2019-10-01") AS res, "2019-10-01" AS date
    FROM
      examples
), oct_second AS (
    SELECT
      udf_add_monthly_engine_searches(prev, curr, "2019-10-02") AS res, "2019-10-02" AS date
    FROM
      examples
)

SELECT
  assert_array_equals(ARRAY [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 5], res.total_searches),
  assert_array_equals(ARRAY [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10], res.tagged_searches),
  assert_array_equals(ARRAY [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15], res.search_with_ads),
  assert_array_equals(ARRAY [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20], res.ad_click)
FROM
  oct_first

UNION ALL

SELECT
  assert_array_equals(ARRAY [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 5], res.total_searches),
  assert_array_equals(ARRAY [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 11], res.tagged_searches),
  assert_array_equals(ARRAY [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15], res.search_with_ads),
  assert_array_equals(ARRAY [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20], res.ad_click)
FROM
  oct_second
