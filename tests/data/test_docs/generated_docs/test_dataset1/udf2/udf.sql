CREATE OR REPLACE FUNCTION test_dataset1.udf2(x INT64) AS (
  test_dataset1.udf1(x, x)
);
