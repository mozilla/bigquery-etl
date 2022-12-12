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
    jsonPayload.logger,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    jsonPayload.fields.device_id,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_stdout_events_v1`
),
unioned AS (
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
)
SELECT
  `timestamp`,
  receiveTimestamp,
  event_category,
  event_type,
  user_id,
  device_id,
  country,
  `language`,
  app_version,
  os_name,
  os_version,
  user_properties,
  event_properties,
  -- extract user properties
  JSON_VALUE(user_properties, "$.utm_term") AS utm_term,
  JSON_VALUE(user_properties, "$.utm_source") AS utm_source,
  JSON_VALUE(user_properties, "$.utm_medium") AS utm_medium,
  JSON_VALUE(user_properties, "$.utm_campaign") AS utm_campaign,
  JSON_VALUE(user_properties, "$.utm_content") AS utm_content,
  JSON_VALUE(user_properties, "$.ua_version") AS ua_version,
  JSON_VALUE(user_properties, "$.ua_browser") AS ua_browser,
  JSON_VALUE(user_properties, "$.entrypoint") AS entrypoint,
  JSON_VALUE(user_properties, "$.entrypoint_experiment") AS entrypoint_experiment,
  JSON_VALUE(user_properties, "$.entrypoint_variation") AS entrypoint_variation,
  JSON_VALUE(user_properties, "$.flow_id") AS flow_id,
  JSON_VALUE(user_properties, "$.sync_device_count") AS sync_device_count,
  JSON_VALUE(user_properties, "$.sync_active_devices_day") AS sync_active_devices_day,
  JSON_VALUE(user_properties, "$.sync_active_devices_week") AS sync_active_devices_week,
  JSON_VALUE(user_properties, "$.sync_active_devices_month") AS sync_active_devices_month,
  -- extract event properties
  JSON_VALUE(event_properties, "$.service") AS service,
  JSON_VALUE(event_properties, "$.email_type") AS email_type,
  JSON_VALUE(event_properties, "$.email_provider") AS email_provider,
  JSON_VALUE(event_properties, "$.oauth_client_id") AS oauth_client_id,
  JSON_VALUE(event_properties, "$.connect_device_flow") AS connect_device_flow,
  JSON_VALUE(event_properties, "$.connect_device_os") AS connect_device_os,
  JSON_VALUE(event_properties, "$.email_sender") AS email_sender,
  JSON_VALUE(event_properties, "$.email_service") AS email_service,
  JSON_VALUE(event_properties, "$.email_template") AS email_template,
  JSON_VALUE(event_properties, "$.email_version") AS email_version,
  JSON_VALUE(event_properties, "$.plan_id") AS plan_id,
  JSON_VALUE(event_properties, "$.product_id") AS product_id,
  JSON_VALUE(event_properties, "$.promotionCode") AS promotion_code,
  JSON_VALUE(event_properties, "$.payment_provider") AS payment_provider,
  JSON_VALUE(event_properties, "$.checkout_type") AS checkout_type,
  JSON_VALUE(event_properties, "$.source_country") AS source_country,
FROM
  unioned
