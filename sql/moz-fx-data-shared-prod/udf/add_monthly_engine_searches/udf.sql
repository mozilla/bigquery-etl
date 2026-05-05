/*
This function specifically windows searches into calendar-month windows. This means groups are not necessarily directly comparable,
since different months have different numbers of days.

On the first of each month, a new month is appended, and the first month is dropped.

If the date is not the first of the month, the new entry is added to the last element in the array.

For example, if we were adding 12 to [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:

On the first of the month, the result would be [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 12]
On any other day of the month, the result would be [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 24]

This happens for every aggregate (searches, ad clicks, etc.)
*/
CREATE OR REPLACE FUNCTION udf.add_monthly_engine_searches(
  prev STRUCT<
    total_searches ARRAY<INT64>,
    tagged_searches ARRAY<INT64>,
    search_with_ads ARRAY<INT64>,
    ad_click ARRAY<INT64>
  >,
  curr STRUCT<
    total_searches ARRAY<INT64>,
    tagged_searches ARRAY<INT64>,
    search_with_ads ARRAY<INT64>,
    ad_click ARRAY<INT64>
  >,
  submission_date DATE
) AS (
  IF(
    EXTRACT(DAY FROM submission_date) = 1,
    STRUCT(
      udf.array_drop_first_and_append(
        prev.total_searches,
        curr.total_searches[OFFSET(11)]
      ) AS total_searches,
      udf.array_drop_first_and_append(
        prev.tagged_searches,
        curr.tagged_searches[OFFSET(11)]
      ) AS tagged_searches,
      udf.array_drop_first_and_append(
        prev.search_with_ads,
        curr.search_with_ads[OFFSET(11)]
      ) AS search_with_ads,
      udf.array_drop_first_and_append(prev.ad_click, curr.ad_click[OFFSET(11)]) AS ad_click
    ),
    STRUCT(
      udf.vector_add(prev.total_searches, curr.total_searches) AS total_searches,
      udf.vector_add(prev.tagged_searches, curr.tagged_searches) AS tagged_searches,
      udf.vector_add(prev.search_with_ads, curr.search_with_ads) AS search_with_ads,
      udf.vector_add(prev.ad_click, curr.ad_click) AS ad_click
    )
  )
);

-- Tests
WITH examples AS (
  SELECT
    STRUCT(
      GENERATE_ARRAY(11, 0, -1) AS total_searches,
      GENERATE_ARRAY(12, 1, -1) AS tagged_searches,
      udf.zeroed_array(12) AS search_with_ads,
      udf.zeroed_array(12) AS ad_click
    ) AS prev,
    STRUCT(
      udf.array_drop_first_and_append(udf.zeroed_array(12), 5) AS total_searches,
      udf.array_drop_first_and_append(udf.zeroed_array(12), 10) AS tagged_searches,
      udf.array_drop_first_and_append(udf.zeroed_array(12), 15) AS search_with_ads,
      udf.array_drop_first_and_append(udf.zeroed_array(12), 20) AS ad_click
    ) AS curr
),
oct_first AS (
  SELECT
    udf.add_monthly_engine_searches(prev, curr, "2019-10-01") AS res,
    "2019-10-01" AS date
  FROM
    examples
),
oct_second AS (
  SELECT
    udf.add_monthly_engine_searches(prev, curr, "2019-10-02") AS res,
    "2019-10-02" AS date
  FROM
    examples
)
SELECT
  mozfun.assert.array_equals([10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 5], res.total_searches),
  mozfun.assert.array_equals([11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10], res.tagged_searches),
  mozfun.assert.array_equals([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15], res.search_with_ads),
  mozfun.assert.array_equals([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20], res.ad_click)
FROM
  oct_first
UNION ALL
SELECT
  mozfun.assert.array_equals([11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 5], res.total_searches),
  mozfun.assert.array_equals([12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 11], res.tagged_searches),
  mozfun.assert.array_equals([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15], res.search_with_ads),
  mozfun.assert.array_equals([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20], res.ad_click)
FROM
  oct_second
