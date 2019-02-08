CREATE TEMP FUNCTION get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map.key_value) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);
