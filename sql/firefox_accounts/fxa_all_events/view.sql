CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_all_events`
AS
WITH fxa_auth_events AS (
  SELECT
    timestamp AS submission_timestamp,
    jsonPayload.fields.user_id,
    jsonPayload.fields.country,
    jsonPayload.fields.language,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    jsonPayload.fields.event_type,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, '$.service') AS service
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_auth_events_v1`
),
  -- This table doesn't include any user events that are considered "active",
  -- but should always be included for a complete raw event log.
fxa_auth_bounce_events AS (
  SELECT
    timestamp AS submission_timestamp,
    jsonPayload.fields.user_id,
    CAST(
      NULL AS STRING
    ) AS country,  -- No country field in auth_bounces
    jsonPayload.fields.language,
    jsonPayload.fields.app_version,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    jsonPayload.fields.event_type,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, '$.service') AS service
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_auth_bounce_events_v1`
),
fxa_content_events AS (
  SELECT
    timestamp AS submission_timestamp,
    jsonPayload.fields.user_id,
    jsonPayload.fields.country,
    jsonPayload.fields.language,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    jsonPayload.fields.event_type,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, '$.service') AS service
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_content_events_v1`
),
fxa_oauth_events AS (
  SELECT
    timestamp AS submission_timestamp,
    jsonPayload.fields.user_id,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS language,
    jsonPayload.fields.app_version,
    CAST(NULL AS STRING) AS os_name,
    CAST(NULL AS STRING) AS os_version,
    jsonPayload.fields.event_type,
    JSON_EXTRACT_SCALAR(jsonPayload.fields.event_properties, '$.service') AS service
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_oauth_events_v1`
)
SELECT
  *
FROM
  fxa_auth_events
UNION ALL
SELECT
  *
FROM
  fxa_auth_bounce_events
UNION ALL
SELECT
  *
FROM
  fxa_content_events
UNION ALL
SELECT
  *
FROM
  fxa_oauth_events
