WITH fxa_events AS (
  SELECT
    `timestamp`,
    receiveTimestamp,
    logger,
    event_type,
    app_version,
    os_name,
    os_version,
    country,
    LANGUAGE,
    user_id,
    utm_term,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    ua_version,
    ua_browser,
    entrypoint,
    entrypoint_experiment,
    entrypoint_variation,
    flow_id,
    service,
    email_type,
    email_provider,
    oauth_client_id,
    connect_device_flow,
    connect_device_os,
    sync_device_count,
    sync_active_devices_day,
    sync_active_devices_week,
    sync_active_devices_month,
    email_sender,
    email_service,
    email_template,
    email_version,
    plan_id,
    product_id,
    promotion_code,
    payment_provider,
    checkout_type,
    source_country,
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
  flow_id IS NOT NULL
GROUP BY
  submission_date,
  flow_id
