SELECT
  * REPLACE ( (
    SELECT
      AS STRUCT jsonPayload.* REPLACE( (
        SELECT
          AS STRUCT jsonPayload.fields.* EXCEPT (device_id,
            user_id),
          TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id) AS fields )) AS jsonPayload )
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth_20*`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.fields.event_type IS NOT NULL
  AND jsonPayload.fields.user_id IS NOT NULL
  AND JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties,
   '$.oauth_client_id') NOT IN (
   -- We filter out events associated with high-volume oauth client IDs that
   -- are redundant with cert_signed events;
   -- see https://github.com/mozilla/bigquery-etl/issues/348
   '3332a18d142636cb', -- fennec sync
   '5882386c6d801776', -- desktop sync
   '1b1a3e44c54fbb58') -- ios sync
  AND _TABLE_SUFFIX = FORMAT_DATE('%g%m%d',
    @submission_date)
