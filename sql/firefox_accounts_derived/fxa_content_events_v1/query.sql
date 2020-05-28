SELECT
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (
          (
            SELECT AS STRUCT
              jsonPayload.fields.* EXCEPT (device_id, user_id),
              TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id
          ) AS fields
        )
    ) AS jsonPayload
  )
FROM
  `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_content_20*`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.fields.event_type IS NOT NULL
  AND _TABLE_SUFFIX = FORMAT_DATE('%g%m%d', @submission_date)
