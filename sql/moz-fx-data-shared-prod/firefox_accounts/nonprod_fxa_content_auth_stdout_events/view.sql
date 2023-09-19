CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.nonprod_fxa_content_auth_stdout_events`
AS
  -- TODO: we should look into deleting this view
  -- as we should use firefox_accounts.nonprod_fxa_all_events instead
  -- to access nonprod fxa event data.
  -- Keeping this around until we can ensure all downstream
  -- tools are updated to use the new view (see: DENG-627).
SELECT
  logger,
  event_type,
  app_version,
  os_name,
  os_version,
  country,
  country_code,
  LANGUAGE,
  user_id,
  device_id,
  `timestamp`,
  receiveTimestamp,
  event_time,
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
  subscription_id,
  plan_id,
  previous_plan_id,
  subscribed_plan_ids,
  product_id,
  previous_product_id,
  promotion_code,
  payment_provider,
  provider_event_id,
  checkout_type,
  source_country,
  country_code_source,
  error_id,
  voluntary_cancellation,
FROM
  `moz-fx-data-shared-prod.firefox_accounts.nonprod_fxa_all_events`
WHERE
  fxa_log IN ("content", "auth", "stdout")
