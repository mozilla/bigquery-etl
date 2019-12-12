/*
An array of 11 zeroes, followed by a supplied value
*/

CREATE TEMP FUNCTION
  udf_array_11_zeroes_then(val INT64)  AS (
    ARRAY [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, val]);
