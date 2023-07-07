SELECT
  SPLIT(jsonPayload.logger, "-")[OFFSET(1)] AS fxa_server,
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (
          (
            SELECT AS STRUCT
              jsonPayload.fields.* EXCEPT (device_id, user_id),
              TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id,
              TO_HEX(SHA256(jsonPayload.fields.device_id)) AS device_id,
              CAST(jsonPayload.fields.t AS STRING) AS t
          ) AS fields
        )
    ) AS jsonPayload
  ),
FROM
  `moz-fx-fxa-nonprod.gke_fxa_stage_log.stdout`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.logger IS NOT NULL
  AND jsonPayload.fields.event_type IS NOT NULL
  AND DATE(`timestamp`) = @submission_date
