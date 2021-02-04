CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.messaging_system.onboarding_retention_events_amplitude`
AS
SELECT
  TIMESTAMP(submission_date) AS submission_timestamp,
  client_id AS device_id,
  CONCAT(client_id, submission_date) AS insert_id,
  "RETENTION" AS event_type,
  TIMESTAMP(submission_date) AS timestamp,
  app_version,
  REGEXP_EXTRACT(os, '^\\w+') AS platform,
  os AS os_name,
  os_version,
  cd.country AS country,
  geo_subdivision1 AS region,
  city,
  -- No `event_properties` for this event
  TO_JSON_STRING(
    STRUCT(
      cd.locale AS locale,
      channel AS release_channel,
      ARRAY(SELECT CONCAT(key, " - ", value) FROM UNNEST(experiments)) AS experiments
    )
  ) AS user_properties
FROM
  `moz-fx-data-shared-prod.messaging_system.onboarding_users_last_seen`
JOIN
  `moz-fx-data-shared-prod.telemetry.clients_daily` cd
USING
  (client_id, submission_date)
