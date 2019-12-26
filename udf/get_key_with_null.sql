/*
Fetch the value associated with a given key from an array of key/value structs.

Because map types aren't available in BigQuery, we model maps as arrays
of structs instead, and this function provides map-like access to such fields.

This version matches NULL keys as well.

*/

CREATE TEMP FUNCTION udf_get_key_with_null(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map) AS key_value
   WHERE key_value.key = k
         OR key_value.key IS NULL and k IS NULL
   LIMIT 1
 )
);

-- Tests

SELECT
  assert_equals(12, udf_get_key_with_null([STRUCT('foo' AS key, 42 AS value), ('bar', 12)], 'bar')),
  assert_equals(12, udf_get_key_with_null([STRUCT('foo' AS key, 42 AS value), (CAST(NULL AS STRING), 12)], CAST(NULL AS STRING)));
