-- FROM: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#from_queries
FROM
  Produce AS p1
JOIN
  Produce AS p2
  USING (item)
|>
  WHERE
    item = 'bananas'
|>
  SELECT
    p1.item,
    p2.sales;

-- SELECT: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#select_pipe_operator
FROM
  (SELECT 'apples' AS item, 2 AS sales)
|>
  SELECT
    item AS fruit_name;

FROM
  Produce
|>
  SELECT
    item,
    sales,
    category,
    SUM(sales) OVER item_window AS category_total
  WINDOW
    item_window AS (
      PARTITION BY
        category
    );

-- EXTEND: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#extend_pipe_operator
FROM
  (SELECT 'apples' AS item, 2 AS sales UNION ALL SELECT 'bananas' AS item, 8 AS sales)
|>
  EXTEND
    item IN ('bananas', 'lemons') AS is_yellow;

FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'carrots' AS item,
      8 AS sales
  )
|>
  EXTEND
    SUM(sales) OVER () AS total_sales;

FROM
  Produce
|>
  EXTEND
    SUM(sales) OVER item_window AS category_total
  WINDOW
    item_window AS (
      PARTITION BY
        category
    );

-- SET: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#set_pipe_operator
FROM
  (SELECT 1 AS x, 11 AS y UNION ALL SELECT 2 AS x, 22 AS y)
|>
  SET
    x = x * x,
    y = 3;

FROM
  (SELECT 2 AS x, 3 AS y) AS t
|>
  SET
    x = x * x,
    y = 8
|>
  SELECT
    t.x AS original_x,
    x,
    y;

-- DROP: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#drop_pipe_operator
FROM
  (SELECT 'apples' AS item, 2 AS sales, 'fruit' AS category)
|>
  DROP
    sales,
    category;

FROM
  (SELECT 1 AS x, 2 AS y) AS t
|>
  DROP
    x
|>
  SELECT
    t.x AS original_x,
    y;

-- RENAME: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#rename_pipe_operator
SELECT
  1 AS x,
  2 AS y,
  3 AS z
|>
  AS t
|>
  RENAME
    y AS renamed_y
|>
  SELECT
    *,
    t.y AS t_y;

-- AS: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#as_pipe_operator
FROM
  (
    SELECT
      "000123" AS id,
      "apples" AS item,
      2 AS sales
    UNION ALL
    SELECT
      "000456" AS id,
      "bananas" AS item,
      5 AS sales
  ) AS sales_table
|>
  AGGREGATE
    SUM(sales) AS total_sales
  GROUP BY
    id,
    item
|>
  AS t1
|>
  JOIN
    (SELECT 456 AS id, "yellow" AS color) AS t2
    ON CAST(t1.id AS INT64) = t2.id
|>
  SELECT
    t2.id,
    total_sales,
    color;

-- WHERE: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#where_pipe_operator
FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'carrots' AS item,
      8 AS sales
  )
|>
  WHERE
    sales >= 3;

-- AGGREGATE: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#aggregate_pipe_operator
FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'apples' AS item,
      7 AS sales
  )
|>
  AGGREGATE
    COUNT(*) AS num_items,
    SUM(sales) AS total_sales;

FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'apples' AS item,
      7 AS sales
  )
|>
  AGGREGATE
    COUNT(*) AS num_items,
    SUM(sales) AS total_sales
  GROUP BY
    item;

FROM
  Produce
|>
  AGGREGATE
    SUM(sales) AS total_sales
  GROUP AND ORDER BY
    category,
    item DESC;

FROM
  Produce
|>
  AGGREGATE
    SUM(sales) AS total_sales ASC
  GROUP BY
    item,
    category DESC;

-- DISTINCT: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#distinct_pipe_operator
FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'carrots' AS item,
      8 AS sales
  )
|>
  DISTINCT
|>
  WHERE
    sales >= 3;

FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'carrots' AS item,
      8 AS sales
  )
|>
  AS Produce
|>
  DISTINCT
|>
  SELECT
    Produce.item;

-- JOIN: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#join_pipe_operator
FROM
  (SELECT 'apples' AS item, 2 AS sales UNION ALL SELECT 'bananas' AS item, 5 AS sales)
|>
  AS produce_sales
|>
  LEFT JOIN
    (SELECT "apples" AS item, 123 AS id) AS produce_data
    ON produce_sales.item = produce_data.item
|>
  SELECT
    produce_sales.item,
    sales,
    id;

-- CALL: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#call_pipe_operator
FROM
  input_table
|>
  CALL tvf1(arg1)
|>
  CALL tvf2(arg2, arg3);

-- ORDER BY: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#order_by_pipe_operator
FROM
  (SELECT 1 AS x UNION ALL SELECT 3 AS x UNION ALL SELECT 2 AS x)
|>
  ORDER BY
    x DESC;

-- LIMIT: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#limit_pipe_operator
FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'carrots' AS item,
      8 AS sales
  )
|>
  ORDER BY
    item
|>
  LIMIT
    1;

FROM
  (
    SELECT
      'apples' AS item,
      2 AS sales
    UNION ALL
    SELECT
      'bananas' AS item,
      5 AS sales
    UNION ALL
    SELECT
      'carrots' AS item,
      8 AS sales
  )
|>
  ORDER BY
    item
|>
  LIMIT
    1 OFFSET 2;

-- UNION: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#union_pipe_operator
SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3]) AS number
|>
  UNION ALL
    (SELECT 1);

SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3]) AS number
|>
  UNION DISTINCT
    (SELECT 1);

SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3]) AS number
|>
  UNION DISTINCT
    (SELECT 1),
    (SELECT 2);

SELECT
  1 AS one_digit,
  10 AS two_digit
|>
  UNION ALL BY NAME
    (SELECT 20 AS two_digit, 2 AS one_digit);

-- INTERSECT: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#intersect_pipe_operator
SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|>
  INTERSECT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[2, 3, 3, 5]) AS number);

SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|>
  INTERSECT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[2, 3, 3, 5]) AS number),
    (SELECT * FROM UNNEST(ARRAY<INT64>[3, 3, 4, 5]) AS number);

WITH NumbersTable AS (
  SELECT
    1 AS one_digit,
    10 AS two_digit
  UNION ALL
  SELECT
    2,
    20
  UNION ALL
  SELECT
    3,
    30
)
SELECT
  one_digit,
  two_digit
FROM
  NumbersTable
|>
  INTERSECT DISTINCT BY NAME
    (SELECT 10 AS two_digit, 1 AS one_digit);

-- EXCEPT: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#except_pipe_operator
SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|>
  EXCEPT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[1, 2]) AS number);

SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|>
  EXCEPT DISTINCT
    (SELECT * FROM UNNEST(ARRAY<INT64>[1, 2]) AS number),
    (SELECT * FROM UNNEST(ARRAY<INT64>[1, 4]) AS number);

SELECT
  *
FROM
  UNNEST(ARRAY<INT64>[1, 2, 3, 3, 4]) AS number
|>
  EXCEPT DISTINCT
    (
      SELECT
        *
      FROM
        UNNEST(ARRAY<INT64>[1, 2]) AS number
      |>
        EXCEPT DISTINCT
          (SELECT * FROM UNNEST(ARRAY<INT64>[1, 4]) AS number)
    );

WITH NumbersTable AS (
  SELECT
    1 AS one_digit,
    10 AS two_digit
  UNION ALL
  SELECT
    2,
    20
  UNION ALL
  SELECT
    3,
    30
)
SELECT
  one_digit,
  two_digit
FROM
  NumbersTable
|>
  EXCEPT DISTINCT BY NAME
    (SELECT 10 AS two_digit, 1 AS one_digit);

-- TABLESAMPLE: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#tablesample_pipe_operator
FROM
  LargeTable
|>
  TABLESAMPLE SYSTEM (1 PERCENT);

-- WITH: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#with_pipe_operator
SELECT
  1 AS key
|>
  WITH t AS (
    SELECT
      1 AS key,
      'my_value' AS value
  )
|>
  INNER JOIN
    t
    USING (key);

SELECT
  1 AS key
|>
  WITH t1 AS (
    SELECT
      2
  ),
  t2 AS (
    SELECT
      3
  )
|>
  UNION ALL
    (FROM t1),
    (FROM t2);

SELECT
  1 AS key
|>
  WITH t1 AS (
    SELECT
      2
  ),
  t2 AS (
    SELECT
      3
  ),
|>
  UNION ALL
    (FROM t1),
    (FROM t2);

-- PIVOT: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#pivot_pipe_operator
FROM
  (
    SELECT
      "kale" AS product,
      51 AS sales,
      "Q1" AS quarter
    UNION ALL
    SELECT
      "kale" AS product,
      4 AS sales,
      "Q1" AS quarter
    UNION ALL
    SELECT
      "kale" AS product,
      45 AS sales,
      "Q2" AS quarter
    UNION ALL
    SELECT
      "apple" AS product,
      8 AS sales,
      "Q1" AS quarter
    UNION ALL
    SELECT
      "apple" AS product,
      10 AS sales,
      "Q2" AS quarter
  )
|>
  PIVOT (SUM(sales) FOR QUARTER IN ('Q1', 'Q2'));

-- UNPIVOT: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#unpivot_pipe_operator
FROM
  (SELECT 'kale' AS product, 55 AS Q1, 45 AS Q2 UNION ALL SELECT 'apple', 8, 10)
|>
  UNPIVOT (sales FOR QUARTER IN (Q1, Q2));

-- MATCH_RECOGNIZE: https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/pipe-syntax#match_recognize_pipe_operator
/* TODO: uncomment after both we and sqlglot add support for MATCH_RECOGNIZE
FROM
  (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3)
|>
  MATCH_RECOGNIZE (
    ORDER BY
      x
    MEASURES
      ARRAY_AGG(high.x) AS high_agg,
      ARRAY_AGG(low.x) AS low_agg
    AFTER MATCH SKIP TO NEXT ROW
    PATTERN
      (low | high)
    DEFINE
      low AS x <= 2,
      high AS x >= 2
  );
*/
