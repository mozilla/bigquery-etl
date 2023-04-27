/*
The function extracts string data from `payload` which is in bytes.
*/
CREATE OR REPLACE FUNCTION  udf_js.extract_string_from_bytes(payload bytes)
returns string as (
  regexp_extract(
    SAFE_CONVERT_BYTES_TO_STRING(payload),
    r'{(.*)}$',
    1
  )
);
    --
SELECT
  assert.array_equals("{test}",udf_js.extract_string_from_bytes(FROM_BASE64("e3Rlc3R9"))),
  assert.array_equals("{}",udf_js.extract_string_from_bytes(FROM_BASE64("e30="))),
  assert.null(null,udf_js.extract_string_from_bytes(FROM_BASE64(null)))
FROM
  extracted


