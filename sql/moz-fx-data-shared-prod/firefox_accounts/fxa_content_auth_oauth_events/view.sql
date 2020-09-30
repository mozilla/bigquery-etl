CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_content_auth_oauth_events`
AS
  --
WITH content AS (
  SELECT
    jsonPayload.logger,
    jsonPayload.fields.event_type,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    jsonPayload.fields.country,
    jsonPayload.fields.language,
    jsonPayload.fields.user_id,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    `timestamp`,
    receiveTimestamp
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_content_events_v1`
),
    --
auth AS (
  SELECT
    jsonPayload.logger,
    jsonPayload.fields.event_type,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    jsonPayload.fields.country,
    jsonPayload.fields.language,
    jsonPayload.fields.user_id,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    `timestamp`,
    receiveTimestamp
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_auth_events_v1`
),
    --
oauth AS (
  SELECT
    jsonPayload.logger,
    jsonPayload.fields.event_type,
    jsonPayload.fields.app_version,
    CAST(NULL AS STRING) AS os_name,
    CAST(NULL AS STRING) AS os_version,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS language,
    jsonPayload.fields.user_id,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    `timestamp`,
    receiveTimestamp
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_oauth_events_v1`
),
    --
unioned AS (
  SELECT
    *
  FROM
    auth
  UNION ALL
  SELECT
    *
  FROM
    content
  UNION ALL
  SELECT
    *
  FROM
    oauth
)
    --
SELECT
  * EXCEPT (user_properties, event_properties),
  REPLACE(JSON_EXTRACT(user_properties, '$.utm_term'), "\"", "") AS utm_term,
  REPLACE(JSON_EXTRACT(user_properties, '$.utm_source'), "\"", "") AS utm_source,
  REPLACE(JSON_EXTRACT(user_properties, '$.utm_medium'), "\"", "") AS utm_medium,
  REPLACE(JSON_EXTRACT(user_properties, '$.utm_campaign'), "\"", "") AS utm_campaign,
  REPLACE(JSON_EXTRACT(user_properties, '$.ua_version'), "\"", "") AS ua_version,
  REPLACE(JSON_EXTRACT(user_properties, '$.ua_browser'), "\"", "") AS ua_browser,
  REPLACE(JSON_EXTRACT(user_properties, '$.entrypoint'), "\"", "") AS entrypoint,
  REPLACE(JSON_EXTRACT(user_properties, '$.flow_id'), "\"", "") AS flow_id,
  REPLACE(JSON_EXTRACT(event_properties, '$.service'), "\"", "") AS service,
  REPLACE(JSON_EXTRACT(event_properties, '$.email_type'), "\"", "") AS email_type,
  REPLACE(JSON_EXTRACT(event_properties, '$.email_provider'), "\"", "") AS email_provider,
  REPLACE(JSON_EXTRACT(event_properties, '$.oauth_client_id'), "\"", "") AS oauth_client_id,
  REPLACE(JSON_EXTRACT(event_properties, '$.connect_device_flow'), "\"", "") AS connect_device_flow,
  REPLACE(JSON_EXTRACT(event_properties, '$.connect_device_os'), "\"", "") AS connect_device_os,
  REPLACE(JSON_EXTRACT(user_properties, '$.sync_device_count'), "\"", "") AS sync_device_count,
  REPLACE(
    JSON_EXTRACT(user_properties, '$.sync_active_devices_day'),
    "\"",
    ""
  ) AS sync_active_devices_day,
  REPLACE(
    JSON_EXTRACT(user_properties, '$.sync_active_devices_week'),
    "\"",
    ""
  ) AS sync_active_devices_week,
  REPLACE(
    JSON_EXTRACT(user_properties, '$.sync_active_devices_month'),
    "\"",
    ""
  ) AS sync_active_devices_month,
  REPLACE(JSON_EXTRACT(event_properties, '$.email_sender'), "\"", "") AS email_sender,
  REPLACE(JSON_EXTRACT(event_properties, '$.email_service'), "\"", "") AS email_service,
  REPLACE(JSON_EXTRACT(event_properties, '$.email_template'), "\"", "") AS email_template,
  REPLACE(JSON_EXTRACT(event_properties, '$.email_version'), "\"", "") AS email_version
FROM
  unioned
