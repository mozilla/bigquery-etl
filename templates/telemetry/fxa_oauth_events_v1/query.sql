SELECT
  * REPLACE ( (
    SELECT
      AS STRUCT jsonPayload.* REPLACE( (
        SELECT
          AS STRUCT jsonPayload.fields.* EXCEPT (user_id),
          TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id) AS fields )) AS jsonPayload )
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_oauth_20*`
WHERE
  (jsonPayload.type LIKE '%.amplitudeEvent' OR jsonPayload.fields.op = 'amplitudeEvent')
  AND jsonPayload.fields.event_type IS NOT NULL
  AND jsonPayload.fields.user_id IS NOT NULL
  AND JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties,
    '$.oauth_client_id') NOT IN ( -- Per https://github.com/mozilla/bigquery-etl/issues/348
    '3332a18d142636cb',
    '5882386c6d801776',
    '1b1a3e44c54fbb58')
  AND _TABLE_SUFFIX = FORMAT_DATE('%g%m%d',
    @submission_date)
