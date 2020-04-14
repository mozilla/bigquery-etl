-- Sampled view on send tab metrics intended for sending to Amplitude;
-- see https://bugzilla.mozilla.org/show_bug.cgi?id=1628740
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.sync_send_tab_events_v1`
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
    SAFE_CAST(
    SAFE_CAST(`moz-fx-data-shared-prod`.udf.get_key(event_map_values, 'serverTime') AS FLOAT64) * 1000 AS INT64
    ) AS time,
    CASE
      event_object
    WHEN
      'processcommand'
    THEN
      'tab_received'
    WHEN
      'sendcommand'
    THEN
      'tab_sent'
    END
    AS event_type,
    payload.uid AS fxa_uid,
    `moz-fx-data-shared-prod`.udf.get_key(event_map_values, 'flowID') AS flow_id,
  FROM
    events
  WHERE
    event_method = 'displayURI'
)
SELECT
  submission_timestamp,
  device_id,
  ARRAY_TO_STRING(
    [device_id, event_category, event_method, event_object, server_time, flow_id],
    '-'
  ) AS insert_id,
  time,
  TIMESTAMP_MILLIS(time) AS timestamp,
  event_type,
  metadata.geo.country,
  metadata.geo.city,
  normalized_os AS os_name,
  normalized_os_version AS os_version,
  FORMAT(
    '{%t}',
    ARRAY_TO_STRING(
      ARRAY(
        SELECT
          FORMAT('"%t":"%t"', key, value)
        FROM
          UNNEST(
            [
              STRUCT('fxa_uid' AS key, fxa_uid AS value),
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
          UNNEST([STRUCT('device_id' AS key, device_id AS value), STRUCT('flow_id', flow_id)])
        WHERE
          value IS NOT NULL
      ),
      ','
    )
  ) AS event_properties,
FROM
  cleaned
WHERE
  -- To save on Amplitude budget, we take a 10% sample based on fxa_uid
  MOD(ABS(FARM_FINGERPRINT(fxa_uid)), 100) < 10
