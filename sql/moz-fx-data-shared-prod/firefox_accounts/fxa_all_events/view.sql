CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_all_events`
AS
WITH fxa_auth_events AS (
  SELECT
    `timestamp`,
    receiveTimestamp,
    jsonPayload.fields.user_id,
    jsonPayload.fields.country,
    jsonPayload.fields.language,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    jsonPayload.fields.event_type,
    JSON_VALUE(jsonPayload.fields.event_properties, '$.service') AS service,
    jsonPayload.logger,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    jsonPayload.fields.device_id,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_auth_events_v1`
),
  -- This table doesn't include any user events that are considered "active",
  -- but should always be included for a complete raw event log.
fxa_auth_bounce_events AS (
  SELECT
    `timestamp`,
    receiveTimestamp,
    jsonPayload.fields.user_id,
    CAST(
      NULL AS STRING
    ) AS country,  -- No country field in auth_bounces
    jsonPayload.fields.language,
    jsonPayload.fields.app_version,
    CAST(NULL AS STRING) AS os_name,
    CAST(NULL AS STRING) AS os_version,
    jsonPayload.fields.event_type,
    JSON_VALUE(jsonPayload.fields.event_properties, '$.service') AS service,
    jsonPayload.logger,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    CAST(NULL AS STRING) AS device_id,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_auth_bounce_events_v1`
),
fxa_content_events AS (
  SELECT
    `timestamp`,
    receiveTimestamp,
    jsonPayload.fields.user_id,
    jsonPayload.fields.country,
    jsonPayload.fields.language,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    jsonPayload.fields.event_type,
    JSON_VALUE(jsonPayload.fields.event_properties, '$.service') AS service,
    jsonPayload.logger,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    jsonPayload.fields.device_id,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_content_events_v1`
),
--
fxa_oauth_events AS (
  SELECT
    `timestamp`,
    receiveTimestamp,
    jsonPayload.fields.user_id,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS language,
    jsonPayload.fields.app_version,
    CAST(NULL AS STRING) AS os_name,
    CAST(NULL AS STRING) AS os_version,
    jsonPayload.fields.event_type,
    JSON_VALUE(jsonPayload.fields.event_properties, '$.service') AS service,
    jsonPayload.logger,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    CAST(NULL AS STRING) AS device_id,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_oauth_events_v1`
),
stdout AS (
  SELECT
    `timestamp`,
    receiveTimestamp,
    jsonPayload.fields.user_id,
    CAST(NULL AS STRING) AS country,
    jsonPayload.fields.language,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    jsonPayload.fields.event_type,
    JSON_VALUE(jsonPayload.fields.event_properties, '$.service') AS service,
    jsonPayload.logger,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    jsonPayload.fields.device_id,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_stdout_events_v1`
)
SELECT
  *,
  'fxa_auth_event' AS event_category,
FROM
  fxa_auth_events
UNION ALL
SELECT
  *,
  'fxa_auth_bounce_event' AS event_category,
FROM
  fxa_auth_bounce_events
UNION ALL
SELECT
  *,
  'fxa_content_event' AS event_category,
FROM
  fxa_content_events
UNION ALL
-- TODO: fxa_oauth_events has been deprecated.
-- This will be removed with another change.
SELECT
  *,
  'fxa_oauth_event' AS event_category,
FROM
  fxa_oauth_events
UNION ALL
SELECT
  *,
  'fxa_stdout_event' AS event_category,
FROM
  stdout
