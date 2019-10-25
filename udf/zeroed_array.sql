CREATE TEMP FUNCTION
  udf_zeroed_array(len INT64) AS (
    ARRAY(
      SELECT 0
      FROM UNNEST(generate_array(1, len))));
