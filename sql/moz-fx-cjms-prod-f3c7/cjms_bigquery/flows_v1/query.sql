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
  flow_id,
  MIN(`timestamp`) AS flow_started,
  ARRAY_AGG(
    IF(
      user_id IS NULL,
      NULL,
      STRUCT(user_id AS fxa_uid, `timestamp` AS fxa_uid_timestamp)
    ) IGNORE NULLS
    ORDER BY
      `timestamp` DESC
    LIMIT
      1
  )[SAFE_OFFSET(0)].*,
FROM
  fxa_events
WHERE
  DATE(`timestamp`) = @submission_date
  AND event_category IN ('fxa_content_event', 'fxa_auth_event', 'fxa_stdout_event')
  AND flow_id IS NOT NULL
GROUP BY
  submission_date,
  flow_id
