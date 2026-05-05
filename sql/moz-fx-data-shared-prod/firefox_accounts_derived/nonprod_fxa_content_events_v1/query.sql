SELECT
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
  )
FROM
  `moz-fx-fxa-nonprod-375e.fxa_stage_logs.docker_fxa_content`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.fields.event_type IS NOT NULL
  AND DATE(`timestamp`) = @submission_date
