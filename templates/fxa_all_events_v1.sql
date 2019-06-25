CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.fxa_all_events_v1`
AS

WITH
  fxa_auth_events AS (
    SELECT
      timestamp AS submission_timestamp,
      jsonPayload.fields.user_id AS user_id,
      jsonPayload.fields.country AS country,
      jsonPayload.fields.event_type AS event_type
    FROM
      `moz-fx-data-derived-datasets.telemetry.fxa_auth_events_v1`
  ),

  -- This table doesn't include any user events that are considered "active",
  -- but should always be included for a complete raw event log.
  fxa_auth_bounce_events AS (
    SELECT
      timestamp AS submission_timestamp,
      jsonPayload.fields.user_id AS user_id,
      CAST(NULL AS STRING) AS country,  -- No country field in auth_bounces
      jsonPayload.fields.event_type AS event_type
    FROM
      `moz-fx-data-derived-datasets.telemetry.fxa_auth_bounce_events_v1`
  ),

  fxa_content_events AS (
    SELECT
      timestamp AS submission_timestamp,
      jsonPayload.fields.user_id AS user_id,
      jsonPayload.fields.country AS country,
      jsonPayload.fields.event_type AS event_type
    FROM
      `moz-fx-data-derived-datasets.telemetry.fxa_content_events_v1`
  )

SELECT * FROM fxa_auth_events
UNION ALL
SELECT * FROM fxa_auth_bounce_events
UNION ALL
SELECT * FROM fxa_content_events
