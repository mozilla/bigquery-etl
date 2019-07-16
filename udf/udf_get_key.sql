/*

Fetch the value associated with a given key from a "map type" imported from
a Parquet dataset.

Our machinery for importing datasets from Parquet models map types as a field
containing a repeated struct named "key_value" with fields key and value.
This function takes in the top level field, so hides the intermediate
"key_value" field.

See udf_get_key_live for a version of this function that is appropriate for use
with "live" ping tables, where we don't have the intermediate key_value struct.

*/

CREATE TEMP FUNCTION udf_get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map.key_value) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);

-- Tests

SELECT
  assert_equals(12, udf_get_key(STRUCT([STRUCT('foo' AS key, 42 AS value), STRUCT('bar', 12)] AS key_value), 'bar'));
