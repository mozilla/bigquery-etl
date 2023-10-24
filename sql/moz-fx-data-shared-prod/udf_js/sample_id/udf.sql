/*

Stably hash a client_id to an integer between 0 and 99.

This function is technically defined in SQL, but it calls a JS UDF
implementation of a CRC-32 hash, so we defined it here to make it
clear that its performance may be limited by BigQuery's JavaScript
UDF environment.

*/
CREATE OR REPLACE FUNCTION udf_js.sample_id(client_id STRING)
RETURNS INT64 AS (
  MOD(udf_js.crc32(client_id), 100)
);

-- Tests
SELECT
  mozfun.assert.equals(15, udf_js.sample_id("b50f76bc-fce4-4345-8d7c-4983f0967488")),
  mozfun.assert.equals(99, udf_js.sample_id("738b7ccd-60b1-47f0-98eb-3d2314559283")),
  mozfun.assert.equals(0, udf_js.sample_id("ed4ed818-63a3-4347-89d7-eae53df14392")),
  mozfun.assert.null(udf_js.sample_id(NULL))
