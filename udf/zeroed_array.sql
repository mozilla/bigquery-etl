/*
Generates an array if all zeroes, of arbitrary length
*/

CREATE OR REPLACE FUNCTION
  udf.zeroed_array(len INT64) AS (
    ARRAY(
      SELECT 0
      FROM UNNEST(generate_array(1, len))));
