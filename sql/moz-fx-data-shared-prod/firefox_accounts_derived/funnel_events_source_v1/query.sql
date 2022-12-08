WITH fxa_events AS (
  SELECT
    * EXCEPT (user_properties, event_properties, service),
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
    `moz-fx-data-shared-prod.firefox_accounts.fxa_all_events`
  WHERE
    DATE(`timestamp`) = @submission_date
    AND event_category IN ('fxa_content_event', 'fxa_auth_event', 'fxa_oauth_event')
)
SELECT
  DATE(`timestamp`) AS submission_date,
  user_id AS client_id,
  `moz-fx-data-shared-prod`.udf.safe_sample_id(user_id) AS sample_id,
  SPLIT(event_type, ' - ')[OFFSET(0)] AS category,
  SPLIT(event_type, ' - ')[OFFSET(1)] AS event,
  [
    STRUCT('service' AS key, service AS value),
    STRUCT('email_type' AS key, email_type AS value),
    STRUCT('oauth_client_id' AS key, oauth_client_id AS value),
    STRUCT('connect_device_flow' AS key, connect_device_flow AS value),
    STRUCT('connect_device_os' AS key, connect_device_os AS value),
    STRUCT('sync_device_count' AS key, sync_device_count AS value),
    STRUCT('email_sender' AS key, email_sender AS value),
    STRUCT('email_service' AS key, email_service AS value),
    STRUCT('email_template' AS key, email_template AS value),
    STRUCT('email_version' AS key, email_version AS value)
  ] AS extra,
  CAST([] AS ARRAY<STRUCT<key STRING, value STRING>>) AS experiments,
  *,
FROM
  fxa_events
