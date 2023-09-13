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
  (
    DATE(_PARTITIONTIME)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 1 DAY)
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  )
  AND DATE(`timestamp`) = @submission_date
  AND jsonPayload.type = 'amplitudeEvent'
  -- We expect to only see events from fxa-auth-server and fxa-content-server here
  -- Although at time of writing they are split across `stdout` and `stderr` logs, there is an open issue to standardize
  -- this: https://mozilla-hub.atlassian.net/browse/FXA-8315
  -- Filtering for both here will ensure we don't miss any events if logger output is changed in the future
  AND jsonPayload.logger IN ("fxa-auth-server", "fxa-content-server")
  AND jsonPayload.fields.event_type IS NOT NULL
  -- The following condition lets through all non-auth-server events, and auth-server events that are not
  -- coming from high-volume oauth client IDs that are redundant. It is copied for compatibility from:
  -- https://github.com/mozilla/bigquery-etl/blob/8a97f747949dc87d9b5425d82776b2c1626aca2e/sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_auth_events_v1/query.sql#L43-L62
  AND (
    jsonPayload.logger != 'fxa-auth-server'
    OR (
      -- We filter out events associated with high-volume oauth client IDs that
      -- are redundant with cert_signed events;
      -- see https://github.com/mozilla/bigquery-etl/issues/348
      JSON_VALUE(jsonPayload.fields.event_properties, '$.oauth_client_id') NOT IN (
        '3332a18d142636cb', -- fennec sync
        '5882386c6d801776', -- desktop sync
        '1b1a3e44c54fbb58' -- ios sync
      )
      -- We do want to let through some desktop sync events
      -- see https://github.com/mozilla/bigquery-etl/issues/573
      OR (
        JSON_VALUE(jsonPayload.fields.event_properties, '$.oauth_client_id') IN (
          '5882386c6d801776',
          '1b1a3e44c54fbb58'
        )
        AND jsonPayload.fields.event_type NOT IN (
          'fxa_activity - access_token_checked',
          'fxa_activity - access_token_created'
        )
      )
      OR JSON_VALUE(jsonPayload.fields.event_properties, '$.oauth_client_id') IS NULL
    )
  )
