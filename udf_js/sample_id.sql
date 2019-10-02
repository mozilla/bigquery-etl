/*

Stably hash a client_id to an integer between 0 and 99.

This function is technically defined in SQL, but it calls a JS UDF
implementation of a CRC-32 hash, so we defined it here to make it
clear that its performance may be limited by BigQuery's JavaScript
UDF environment.

*/

CREATE TEMP FUNCTION
  udf_js_sample_id(client_id STRING)
  RETURNS INT64 AS (
    MOD(udf_js_crc32(client_id), 100)
  );

-- Tests

SELECT
  assert_equals(15, udf_js_sample_id("b50f76bc-fce4-4345-8d7c-4983f0967488")),
  assert_equals(99, udf_js_sample_id("738b7ccd-60b1-47f0-98eb-3d2314559283")),
  assert_equals(0, udf_js_sample_id("ed4ed818-63a3-4347-89d7-eae53df14392")),
  assert_null(udf_js_sample_id(null))
