/*
Fetch the value associated with a given key from an array of key/value structs.

Because map types aren't available in BigQuery, we model maps as arrays
of structs instead, and this function provides map-like access to such fields.

*/
CREATE OR REPLACE FUNCTION map.get_key(map ANY TYPE, k ANY TYPE) AS (
  (SELECT key_value.value FROM UNNEST(map) AS key_value WHERE key_value.key = k LIMIT 1)
);

-- Tests
SELECT
  assert.equals(12, map.get_key([STRUCT('foo' AS key, 42 AS value), ('bar', 12)], 'bar'));
