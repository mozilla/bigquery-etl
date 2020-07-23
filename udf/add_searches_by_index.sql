/*

Return sums of each search type grouped by the index.  Results are ordered by index.

 */
CREATE OR REPLACE FUNCTION udf.add_searches_by_index(
  searches ARRAY<
    STRUCT<
      total_searches INT64,
      tagged_searches INT64,
      search_with_ads INT64,
      ad_click INT64,
      index INT64
    >
  >
) AS (
  (
    WITH summed_by_index AS (
      SELECT AS STRUCT
        SUM(search.total_searches) AS total_searches,
        SUM(search.tagged_searches) AS tagged_searches,
        SUM(search.search_with_ads) AS search_with_ads,
        SUM(search.ad_click) AS ad_click,
      FROM
        UNNEST(searches) search
      GROUP BY
        index
      ORDER BY
        index
    )
    SELECT AS STRUCT
      ARRAY_AGG(total_searches) total_searches,
      ARRAY_AGG(tagged_searches) tagged_searches,
      ARRAY_AGG(search_with_ads) search_with_ads,
      ARRAY_AGG(ad_click) ad_click,
    FROM
      summed_by_index
  )
);

-- Test
WITH correct_order AS (
  SELECT
    udf.add_searches_by_index(
      ARRAY<STRUCT<INT64, INT64, INT64, INT64, INT64>>[
        (3, 3, 0, 3, 3),
        (0, 1, 1, 1, 1),
        (4, 4, 4, 0, 4),
        (2, 0, 2, 2, 2)
      ]
    ) AS actual
),
correct_sum AS (
  SELECT
    udf.add_searches_by_index(
      ARRAY<STRUCT<INT64, INT64, INT64, INT64, INT64>>[
        (1, 1, 1, 1, 3),
        (1, 2, 3, 4, 1),
        (1, 2, 3, 4, 1),
        (1, 1, 1, 1, 1),
        (4, 4, 4, 4, 2),
        (2, 3, 4, 5, 2)
      ]
    ) AS actual
)
SELECT
  assert_array_equals([0, 2, 3, 4], actual.total_searches),
  assert_array_equals([1, 0, 3, 4], actual.tagged_searches),
  assert_array_equals([1, 2, 0, 4], actual.search_with_ads),
  assert_array_equals([1, 2, 3, 0], actual.ad_click),
FROM
  correct_order
UNION ALL
SELECT
  assert_array_equals([3, 6, 1], actual.total_searches),
  assert_array_equals([5, 7, 1], actual.tagged_searches),
  assert_array_equals([7, 8, 1], actual.search_with_ads),
  assert_array_equals([9, 9, 1], actual.ad_click),
FROM
  correct_sum
