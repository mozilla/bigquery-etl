SELECT
  -- example logger expected input: fxa-auth-server
  SPLIT(jsonPayload.logger, "-")[OFFSET(1)] AS fxa_server,
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (
          (
            SELECT AS STRUCT
              jsonPayload.fields.* REPLACE (
                TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id,
                TO_HEX(SHA256(jsonPayload.fields.device_id)) AS device_id
              )
          ) AS fields
        )
    ) AS jsonPayload
  ),
FROM
  `moz-fx-fxa-prod.gke_fxa_prod_log.stderr`
WHERE
  -- TODO: Looks like partition field is different as expected, need to confirm with SRE that this is intended.
  -- DATE(`timestamp`) = @submission_date
  (
    DATE(_PARTITIONTIME)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  )
  AND DATE(`timestamp`) = @submission_date
  AND jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.logger IS NOT NULL
  AND jsonPayload.fields.event_type IS NOT NULL
  AND (
    jsonPayload.logger != 'fxa-auth-server'
    OR (
             -- We filter out events associated with high-volume oauth client IDs that
             -- are redundant with cert_signed events;
             -- see https://github.com/mozilla/bigquery-etl/issues/348
      JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, '$.oauth_client_id') NOT IN (
        '3332a18d142636cb', -- fennec sync
        '5882386c6d801776', -- desktop sync
        '1b1a3e44c54fbb58' -- ios sync
      )
             -- We do want to let through some desktop sync events
             -- see https://github.com/mozilla/bigquery-etl/issues/573
      OR (
        JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, '$.oauth_client_id') IN (
          '5882386c6d801776',
          '1b1a3e44c54fbb58'
        )
        AND jsonPayload.fields.event_type NOT IN (
          'fxa_activity - access_token_checked',
          'fxa_activity - access_token_created'
        )
      )
      OR JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, '$.oauth_client_id') IS NULL
    )
  )
