CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.nonprod_fxa_content_auth_stdout_events`
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
    `moz-fx-data-shared-prod.firefox_accounts_derived.nonprod_fxa_content_events_v1`
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
    `moz-fx-data-shared-prod.firefox_accounts_derived.nonprod_fxa_auth_events_v1`
),
  --
stdout AS (
  SELECT
    jsonPayload.logger,
    jsonPayload.fields.event_type,
    jsonPayload.fields.app_version,
    jsonPayload.fields.os_name,
    jsonPayload.fields.os_version,
    CAST(NULL AS STRING) AS country,
    CAST(NULL AS STRING) AS language,
    jsonPayload.fields.user_id,
    jsonPayload.fields.user_properties,
    jsonPayload.fields.event_properties,
    `timestamp`,
    receiveTimestamp
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.nonprod_fxa_stdout_events_v1`
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
    stdout
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
  JSON_VALUE(user_properties, "$.entrypoint_experiment") AS entrypoint_experiment,
  JSON_VALUE(user_properties, "$.entrypoint_variation") AS entrypoint_variation,
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
  JSON_VALUE(event_properties, "$.plan_id") AS plan_id,
  JSON_VALUE(event_properties, "$.product_id") AS product_id,
  JSON_VALUE(event_properties, "$.promotionCode") AS promotion_code,
  JSON_VALUE(event_properties, "$.paymentProvider") AS payment_provider,
  JSON_VALUE(event_properties, "$.selectedPlan") AS selected_plan,
FROM
  unioned
