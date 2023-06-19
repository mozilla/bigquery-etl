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
  ),
  SPLIT(jsonPayload.logger, "-")[OFFSET(1)] AS fxa_server,
FROM
  `moz-fx-fxa-nonprod.gke_fxa_stage_log.stdout`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.logger IS NOT NULL
  AND jsonPayload.fields.event_type IS NOT NULL
  AND DATE(`timestamp`) = @submission_date
UNION ALL
-- Pulling data from the old stdout table too as some of the GCP
-- fxa events are still landing inside it.
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
  ),
  SPLIT(jsonPayload.logger, "-")[OFFSET(1)] AS fxa_server,
FROM
  `moz-fx-fxa-nonprod-375e.fxa_stage_logs.stdout`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.logger IS NOT NULL
  AND jsonPayload.fields.event_type IS NOT NULL
  AND DATE(`timestamp`) = @submission_date
