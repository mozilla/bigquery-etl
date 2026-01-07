SELECT
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (
          (
            SELECT AS STRUCT
              jsonPayload.fields.* EXCEPT (device_id, user_id),
              TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id,
              TO_HEX(SHA256(jsonPayload.fields.device_id)) AS device_id
          ) AS fields
        )
    ) AS jsonPayload
  )
FROM
  `moz-fx-data-shared-prod.firefox_accounts_prod_logs_syndicate.docker_fxa_auth_bounces`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.fields.event_type IS NOT NULL
  AND jsonPayload.fields.user_id IS NOT NULL
  AND DATE(`timestamp`) = @submission_date
