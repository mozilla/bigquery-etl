CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.messaging_system.onboarding_events_amplitude`
AS
SELECT
  submission_timestamp,
  client_id AS device_id,
  document_id AS insert_id,
  event AS event_type,
  submission_timestamp AS timestamp,
  version AS app_version,
  metadata.user_agent.os AS platform,
  metadata.user_agent.os AS os_name,
  normalized_os_version AS os_version,
  NULL AS device_manufacturer,
  NULL AS device_model,
  metadata.geo.country AS country,
  metadata.geo.subdivision1 AS region,
  metadata.geo.city AS city,
  ( -- `event_context` should already be a JSON string, the IFNULL guard is only
    -- for the old Firefox versions.
    `moz-fx-data-shared-prod.udf.kv_array_append_to_json_string`(
      IFNULL(event_context, "{}"),
      [STRUCT("message_id" AS key, message_id AS value)]
    )
  ) AS event_properties,
  (
    `moz-fx-data-shared-prod.udf.kv_array_to_json_string`(
      ARRAY_CONCAT(
        [STRUCT("locale" AS key, locale AS value)],
        [STRUCT("release_channel" AS key, release_channel AS value)],
        ARRAY(SELECT STRUCT(key AS key, value.branch AS value) FROM UNNEST(experiments))
      )
    )
  ) AS user_properties
FROM
  `moz-fx-data-shared-prod.messaging_system_stable.onboarding_v1`
