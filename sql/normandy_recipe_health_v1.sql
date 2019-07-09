CREATE TEMP FUNCTION udf_get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map.key_value) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);
--
SELECT
  @submission_date as submission_date,
  SUBSTR(udf_get_key(e.event_map_values, 'source'), CHAR_LENGTH('normandy/recipe/') + 1) AS recipe,
  e.event_string_value AS status,
  COUNT(*) AS count
FROM
  events_v1 e
WHERE
  submission_date_s3 = @submission_date AND
  sample_id = '6' AND
  e.event_category = 'uptake.remotecontent.result' AND
  udf_get_key(e.event_map_values, 'source') LIKE 'normandy/recipe/%'
GROUP BY
  recipe,
  status
