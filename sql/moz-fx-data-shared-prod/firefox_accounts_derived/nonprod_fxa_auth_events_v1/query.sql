SELECT
  * REPLACE (
    (
      SELECT AS STRUCT
        jsonPayload.* REPLACE (
          (
            SELECT AS STRUCT
              jsonPayload.fields.* EXCEPT (device_id, user_id) REPLACE(
                -- See https://bugzilla.mozilla.org/show_bug.cgi?id=1707571
                CAST(NULL AS FLOAT64) AS emailverified,
                CAST(NULL AS FLOAT64) AS isprimary,
                CAST(NULL AS FLOAT64) AS isverified,
                -- casting id as field type in source tables inconsistent
                CAST(jsonPayload.fields.id AS STRING) AS id
              ),
              TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id
          ) AS fields
        )
    ) AS jsonPayload
  )
FROM
  `moz-fx-fxa-nonprod-375e.fxa_stage_logs.docker_fxa_auth_20*`
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.fields.event_type IS NOT NULL
  AND jsonPayload.fields.user_id IS NOT NULL
  AND PARSE_DATE('%y%m%d', _TABLE_SUFFIX) = DATE(@submission_date)
