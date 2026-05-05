/*
Stub for a function that returns whether search is monetized.
*/
CREATE OR REPLACE FUNCTION udf.monetized_search(
  engine STRING,
  country STRING,
  distribution_id STRING,
  submission_date DATE
) AS (
  CASE
    WHEN TRUE
      THEN FALSE
  END
);
