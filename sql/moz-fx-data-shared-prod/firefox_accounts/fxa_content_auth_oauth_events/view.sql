-----
--- ** DEPRECATION NOTICE **
---
--- There is currently an ongoing effort to deprecate this view
--- please use `firefox_accounts.fxa_all_events` view instead
--- in new queries.
---
--- Please filter on `fxa_log` field to limit your results
--- to events coming only from a specific fxa server like so:
--- WHERE fxa_log IN ('auth', 'stdout', ...)
--- Options include:
---   content
---   auth
---   stdout
---   oauth -- this has been deprecated and merged into fxa_auth_event
---   auth_bounce
--- to replicate results of this view use:
--- WHERE fxa_log IN (
---  'content',
---  'auth',
--   'oauth'
--- )
-----
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_content_auth_oauth_events`
AS
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
    jsonPayload.fields.device_id,
    `timestamp`,
    receiveTimestamp,
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
    jsonPayload.fields.device_id,
    `timestamp`,
    receiveTimestamp,
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
    CAST(NULL AS STRING) AS device_id,
    `timestamp`,
    receiveTimestamp,
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
  JSON_VALUE(user_properties, "$.utm_term") AS utm_term,
  JSON_VALUE(user_properties, "$.utm_source") AS utm_source,
  JSON_VALUE(user_properties, "$.utm_medium") AS utm_medium,
  JSON_VALUE(user_properties, "$.utm_campaign") AS utm_campaign,
  JSON_VALUE(user_properties, "$.utm_content") AS utm_content,
  JSON_VALUE(user_properties, "$.ua_version") AS ua_version,
  JSON_VALUE(user_properties, "$.ua_browser") AS ua_browser,
  JSON_VALUE(user_properties, "$.entrypoint") AS entrypoint,
  JSON_VALUE(user_properties, "$.flow_id") AS flow_id,
  JSON_VALUE(event_properties, "$.service") AS service,
  JSON_VALUE(event_properties, "$.email_type") AS email_type,
  JSON_VALUE(event_properties, "$.email_provider") AS email_provider,
  JSON_VALUE(event_properties, "$.oauth_client_id") AS oauth_client_id,
  JSON_VALUE(event_properties, "$.connect_device_flow") AS connect_device_flow,
  JSON_VALUE(event_properties, "$.connect_device_os") AS connect_device_os,
  JSON_VALUE(user_properties, "$.sync_device_count") AS sync_device_count,
  JSON_VALUE(user_properties, "$.sync_active_devices_day") AS sync_active_devices_day,
  JSON_VALUE(user_properties, "$.sync_active_devices_week") AS sync_active_devices_week,
  JSON_VALUE(user_properties, "$.sync_active_devices_month") AS sync_active_devices_month,
  JSON_VALUE(event_properties, "$.email_sender") AS email_sender,
  JSON_VALUE(event_properties, "$.email_service") AS email_service,
  JSON_VALUE(event_properties, "$.email_template") AS email_template,
  JSON_VALUE(event_properties, "$.email_version") AS email_version,
FROM
  unioned
-- Commented out for now, to restore FxA Looker dashboards
-- Once dashboards have been migrated to use fxa_all_events view
-- this will be uncommented to see if we can pick up any other usage
-- of this view.
-- See DENG-582 for more info.
-- WHERE
--   ERROR(
--     'VIEW DEPRECATED - This view will be completely deleted after 9th of February 2023, please use `fxa_all_events` with filter on `fxa_log` instead. See DENG-582 for more info.'
--   )
