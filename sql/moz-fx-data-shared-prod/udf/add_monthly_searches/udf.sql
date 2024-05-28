/*
Adds together two engine searches structs. Each engine searches struct has a MAP[engine -> search_counts_struct].
We want to add add together the prev and curr's values for a certain engine.

This allows us to be flexible with the number of engines we're using.
*/
CREATE OR REPLACE FUNCTION udf.add_monthly_searches(
  prev ARRAY<
    STRUCT<
      key STRING,
      value STRUCT<
        total_searches ARRAY<INT64>,
        tagged_searches ARRAY<INT64>,
        search_with_ads ARRAY<INT64>,
        ad_click ARRAY<INT64>
      >
    >
  >,
  curr ARRAY<
    STRUCT<
      key STRING,
      value STRUCT<
        total_searches ARRAY<INT64>,
        tagged_searches ARRAY<INT64>,
        search_with_ads ARRAY<INT64>,
        ad_click ARRAY<INT64>
      >
    >
  >,
  submission_date DATE
) AS (
  ARRAY(
    WITH prev_tbl AS (
      SELECT
        * REPLACE (COALESCE(key, "null_engine") AS key)
      FROM
        UNNEST(prev)
    ),
    curr_tbl AS (
      SELECT
        * REPLACE (COALESCE(key, "null_engine") AS key)
      FROM
        UNNEST(curr)
    )
    SELECT
      STRUCT(
        NULLIF(key, "null_engine") AS key,
        udf.add_monthly_engine_searches(
          COALESCE(p.value, udf.new_monthly_engine_searches_struct()),
          COALESCE(c.value, udf.new_monthly_engine_searches_struct()),
          submission_date
        ) AS value
      )
    FROM
      curr_tbl AS c
    FULL OUTER JOIN
      prev_tbl AS p
      USING (key)
  )
);

-- Tests
/*
NOTE: These tests are structured in this way to distribute them.
      If they were done as one row, they all run on a single
      node and take >10 minutes to complete.
*/
WITH previous_examples AS (
  SELECT
    [
      STRUCT(
        "google" AS key,
        STRUCT(
          GENERATE_ARRAY(11, 0, -1) AS total_searches,
          GENERATE_ARRAY(12, 1, -1) AS tagged_searches,
          udf.zeroed_array(12) AS search_with_ads,
          udf.zeroed_array(12) AS ad_click
        ) AS value
      )
    ] AS prev,
    "google" AS type
  UNION ALL
  SELECT
    [
      STRUCT(CAST(NULL AS STRING) AS key, udf.new_monthly_engine_searches_struct() AS value)
    ] AS prev,
    "null" AS type
),
current_examples AS (
  SELECT
    [
      STRUCT(
        "google" AS key,
        STRUCT(
          udf.array_drop_first_and_append(udf.zeroed_array(12), 5) AS total_searches,
          udf.array_drop_first_and_append(udf.zeroed_array(12), 10) AS tagged_searches,
          udf.array_drop_first_and_append(udf.zeroed_array(12), 15) AS search_with_ads,
          udf.array_drop_first_and_append(udf.zeroed_array(12), 20) AS ad_click
        ) AS value
      )
    ] AS curr,
    "google" AS type
  UNION ALL
  SELECT
    ARRAY[
      STRUCT(
        "bing" AS key,
        STRUCT(
          udf.array_drop_first_and_append(udf.zeroed_array(12), 1) AS total_searches,
          udf.array_drop_first_and_append(udf.zeroed_array(12), 2) AS tagged_searches,
          udf.array_drop_first_and_append(udf.zeroed_array(12), 3) AS search_with_ads,
          udf.array_drop_first_and_append(udf.zeroed_array(12), 4) AS ad_click
        ) AS value
      )
    ] AS curr,
    "bing" AS type
  UNION ALL
  SELECT
    [
      STRUCT(CAST(NULL AS STRING) AS key, udf.new_monthly_engine_searches_struct() AS value)
    ] AS curr,
    "null" AS type
),
dates AS (
  SELECT
    d AS date
  FROM
    UNNEST([DATE "2019-10-01", DATE "2019-10-02"]) AS d
),
results AS (
  SELECT
    udf.add_monthly_searches(p.prev, c.curr, d.date) AS res,
    p.type AS p_type,
    c.type AS c_type,
    d.date AS date
  FROM
    previous_examples p
  CROSS JOIN
    current_examples c
  CROSS JOIN
    dates d
),
expected AS (
  SELECT
    *
  FROM
    UNNEST(
      [
      -- First day of the month, join new day of same engine
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "total_searches" AS res_type,
          [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 5] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "tagged_searches" AS res_type,
          [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 10] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "search_with_ads" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "ad_click" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20] AS exp
        ),
      -- Second day of the month, join new day of same engine
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-02" AS date,
          "google" AS key,
          "total_searches" AS res_type,
          [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 5] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-02" AS date,
          "google" AS key,
          "tagged_searches" AS res_type,
          [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 11] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-02" AS date,
          "google" AS key,
          "search_with_ads" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "google" AS c_type,
          DATE "2019-10-02" AS date,
          "google" AS key,
          "ad_click" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20] AS exp
        ),
      -- Join new client of data
        STRUCT(
          "null" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "total_searches" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5] AS exp
        ),
        STRUCT(
          "null" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "tagged_searches" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10] AS exp
        ),
        STRUCT(
          "null" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "search_with_ads" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15] AS exp
        ),
        STRUCT(
          "null" AS p_type,
          "google" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "ad_click" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20] AS exp
        ),
      -- Join existing client without any new data
        STRUCT(
          "google" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "total_searches" AS res_type,
          [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "tagged_searches" AS res_type,
          [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "search_with_ads" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "ad_click" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        ),
      -- Join existing client with data from new engine (check both engines)
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "total_searches" AS res_type,
          [10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "tagged_searches" AS res_type,
          [11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "search_with_ads" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "google" AS key,
          "ad_click" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        ),
      -- Checking second engine
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "bing" AS key,
          "total_searches" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "bing" AS key,
          "tagged_searches" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "bing" AS key,
          "search_with_ads" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3] AS exp
        ),
        STRUCT(
          "google" AS p_type,
          "bing" AS c_type,
          DATE "2019-10-01" AS date,
          "bing" AS key,
          "ad_click" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4] AS exp
        ),
      -- Check NULL join
        STRUCT(
          "null" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          NULL AS key,
          "total_searches" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        ),
        STRUCT(
          "null" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          NULL AS key,
          "tagged_searches" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        ),
        STRUCT(
          "null" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          NULL AS key,
          "search_with_ads" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        ),
        STRUCT(
          "null" AS p_type,
          "null" AS c_type,
          DATE "2019-10-01" AS date,
          NULL AS key,
          "ad_click" AS res_type,
          [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] AS exp
        )
      ]
    )
)
SELECT
  mozfun.assert.array_equals(
    exp,
    CASE
      WHEN res_type = "total_searches"
        THEN udf.get_key_with_null(res, key).total_searches
      WHEN res_type = "tagged_searches"
        THEN udf.get_key_with_null(res, key).tagged_searches
      WHEN res_type = "search_with_ads"
        THEN udf.get_key_with_null(res, key).search_with_ads
      WHEN res_type = "ad_click"
        THEN udf.get_key_with_null(res, key).ad_click
    END
  ),
FROM
  results
INNER JOIN
  expected
  USING (p_type, c_type, date);
