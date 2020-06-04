/*
Sum an array from a paquet-derived field.
These lists of an `element` that represents the field.
*/

CREATE OR REPLACE FUNCTION udf.parquet_array_sum(list ANY TYPE) AS (
  (SELECT SUM(x.element) FROM UNNEST(list) AS x)
);

-- Tests
SELECT
  assert_equals(10, udf.parquet_array_sum([STRUCT(5 AS element), STRUCT(5 AS element)]))
