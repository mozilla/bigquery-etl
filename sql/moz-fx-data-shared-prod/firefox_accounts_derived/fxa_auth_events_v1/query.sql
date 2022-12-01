WITH base AS (
  SELECT
    JSON_EXTRACT_SCALAR(
      jsonPayload.fields.event_properties,
      '$.oauth_client_id'
    ) AS _oauth_client_id,
    * REPLACE (
      (
        SELECT AS STRUCT
          jsonPayload.* REPLACE (
            (
              SELECT AS STRUCT
                jsonPayload.fields.* EXCEPT (user_id, device_id, deviceid) REPLACE(
                  -- See https://bugzilla.mozilla.org/show_bug.cgi?id=1707571
                  CAST(NULL AS FLOAT64) AS emailverified,
                  CAST(NULL AS FLOAT64) AS isprimary,
                  CAST(NULL AS FLOAT64) AS isverified,
                  CAST(NULL AS STRING) AS id,
                  CAST(NULL AS STRING) AS metricsoptoutat
                ),
                TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id,
                TO_HEX(
                  SHA256(COALESCE(jsonPayload.fields.device_id, jsonPayload.fields.deviceid))
                ) AS device_id
            ) AS fields
          )
      ) AS jsonPayload
    )
  FROM
    `moz-fx-fxa-prod-0712.fxa_prod_logs.docker_fxa_auth_20*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%y%m%d', @submission_date)
)
  --
SELECT
  * EXCEPT (_oauth_client_id)
FROM
  base
WHERE
  jsonPayload.type = 'amplitudeEvent'
  AND jsonPayload.fields.event_type IS NOT NULL
  AND jsonPayload.fields.user_id IS NOT NULL
  AND (
    -- We filter out events associated with high-volume oauth client IDs that
    -- are redundant with cert_signed events;
    -- see https://github.com/mozilla/bigquery-etl/issues/348
    _oauth_client_id NOT IN (
      '3332a18d142636cb', -- fennec sync
      '5882386c6d801776', -- desktop sync
      '1b1a3e44c54fbb58' -- ios sync
    )
    -- We do want to let through some desktop sync events
    -- see https://github.com/mozilla/bigquery-etl/issues/573
    OR (
      _oauth_client_id IN ('5882386c6d801776', '1b1a3e44c54fbb58')
      AND jsonPayload.fields.event_type NOT IN (
        'fxa_activity - access_token_checked',
        'fxa_activity - access_token_created'
      )
    )
    OR _oauth_client_id IS NULL
  )
