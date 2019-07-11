SELECT
  @submission_date as submission_date,
  e.event_string_value AS status,
  COUNT(*) AS count
FROM
  events_v1 e
WHERE
  submission_date_s3 = @submission_date AND
  sample_id = '6' AND
  e.event_category = 'uptake.remotecontent.result' AND
  udf_get_key(e.event_map_values, 'source') = 'normandy/runner'
GROUP BY
  recipe,
  status
