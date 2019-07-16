/*

Fetch the value associated with a given key from an array of key/value structs.

Because map types aren't available in BigQuery, we model maps as arrays
of structs instead, and this function provides map-like access to such fields.

Also see udf_get_key, which has a very similar function, but is optimized for
use on tables imported from Parquet datasets, where map types have an extra
level of nesting. We will in the future be retiring that format, at which
point udf_get_key will be replaced with the implementation here and
udf_get_key_live will be deprecated as redundant.

*/

CREATE TEMP FUNCTION udf_get_key_live(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);

-- Tests

SELECT
  assert_equals(12, udf_get_key_live([STRUCT('foo' AS key, 42 AS value), STRUCT('bar', 12)], 'bar'));
