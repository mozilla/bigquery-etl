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
  REGEXP_EXTRACT(metadata.user_agent.os, '^\\w+') AS platform,
  metadata.user_agent.os AS os_name,
  normalized_os_version AS os_version,
  metadata.geo.country AS country,
  metadata.geo.subdivision1 AS region,
  metadata.geo.city AS city,
  (
    `moz-fx-data-shared-prod.udf.kv_array_append_to_json_string`(
      event_context,
      [STRUCT("message_id" AS key, message_id AS value)]
    )
  ) AS event_properties,
  TO_JSON_STRING(
    STRUCT(
      locale,
      release_channel,
      ARRAY(SELECT CONCAT(key, " - ", value.branch) FROM UNNEST(experiments)) AS experiments,
      attribution.source AS attribution_source,
      attribution.ua AS attribution_ua
    )
  ) AS user_properties
FROM
  `moz-fx-data-shared-prod.messaging_system_stable.onboarding_v1`
WHERE
  -- Fetch events on about:welcome only to minimize the event volume
  event_context LIKE "%about:welcome%"
