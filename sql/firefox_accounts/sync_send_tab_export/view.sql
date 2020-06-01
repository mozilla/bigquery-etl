-- Sampled view on send tab metrics intended for sending to Amplitude;
-- see https://bugzilla.mozilla.org/show_bug.cgi?id=1628740
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.sync_send_tab_export`
AS
WITH events AS (
  SELECT
    *,
    `moz-fx-data-shared-prod`.udf.deanonymize_event(event).*
  FROM
    `moz-fx-data-shared-prod.telemetry.sync`
  CROSS JOIN
    UNNEST(payload.events) AS event
),
cleaned AS (
  SELECT
    *,
    payload.device_id,
    `moz-fx-data-shared-prod`.udf.get_key(event_map_values, 'serverTime') AS server_time,
    `moz-fx-data-shared-prod`.udf.normalize_os(payload.os.name) AS os_name,
    CASE
      event_object
    WHEN
      'processcommand'
    THEN
      'sync - tab_received'
    WHEN
      'sendcommand'
    THEN
      'sync - tab_sent'
    END
    AS event_type,
    `moz-fx-data-shared-prod`.udf.get_key(event_map_values, 'flowID') AS flow_id,
  FROM
    events
  WHERE
    event_method = 'displayURI'
)
SELECT
  cleaned.submission_timestamp,
  ids.user_id,
  device_id,
  ARRAY_TO_STRING(
    [device_id, event_category, event_method, event_object, server_time, flow_id],
    '-'
  ) AS insert_id,
  -- Amplitude expects a `time` field in milliseconds since UNIX epoch.
  COALESCE(
    -- server_time is in seconds, but with one digit after the decimal place, so we
    -- have to cast to float, multiply to get milliseconds, then cast to int.
    SAFE_CAST(SAFE_CAST(server_time AS FLOAT64) * 1000 AS INT64),
    -- server_time is sometimes null, so we fall back to submission_timestamp
    UNIX_MILLIS(cleaned.submission_timestamp)
  ) AS time,
  event_type,
  metadata.geo.country,
  metadata.geo.city,
  os_name,
  payload.os.version AS os_version,
  payload.os.locale AS `language`,
  FORMAT(
    '{%t}',
    ARRAY_TO_STRING(
      ARRAY(
        SELECT
          FORMAT('"%t":"%t"', key, value)
        FROM
          UNNEST(
            [
              STRUCT('fxa_uid' AS key, ids.user_id AS value),
              STRUCT('ua_browser', metadata.user_agent.browser),
              STRUCT('ua_version', metadata.user_agent.version)
            ]
          )
        WHERE
          value IS NOT NULL
      ),
      ','
    )
  ) AS user_properties,
  FORMAT(
    '{%t}',
    ARRAY_TO_STRING(
      ARRAY(
        SELECT
          FORMAT('"%t":"%t"', key, value)
        FROM
          UNNEST([STRUCT('flow_id' AS key, flow_id AS value)])
        WHERE
          value IS NOT NULL
      ),
      ','
    )
  ) AS event_properties,
FROM
  cleaned
-- We need this join because sync pings contain a truncated ID that is just the
-- first 32 characters of the 64-character hash sent to Amplitude by other producers;
-- we join based on the prefix to recover the full 64-character hash.
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_amplitude_user_ids_v1` AS ids
ON
  cleaned.payload.uid = SUBSTR(ids.user_id, 1, 32)
WHERE
  -- To save on Amplitude budget, we take a 10% sample based on user ID.
  MOD(ABS(FARM_FINGERPRINT(payload.uid)), 100) < 10
