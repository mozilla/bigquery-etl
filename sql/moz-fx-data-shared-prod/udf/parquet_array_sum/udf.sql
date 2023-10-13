/*
Sum an array from a parquet-derived field.
These are lists of an `element` that contain the field value.
*/
CREATE OR REPLACE FUNCTION udf.parquet_array_sum(list ANY TYPE) AS (
  (SELECT SUM(x.element) FROM UNNEST(list) AS x)
);

-- Tests
SELECT
  mozfun.assert.equals(10, udf.parquet_array_sum([STRUCT(5 AS element), STRUCT(5 AS element)]))
