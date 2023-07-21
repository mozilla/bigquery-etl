SELECT
  submission_timestamp,
  additional_properties,
  metrics.string.messaging_system_addon_version AS addon_version,
  metrics.uuid.messaging_system_client_id AS client_id,
  document_id,
  metrics.string.messaging_system_event AS event,
  metrics.text2.messaging_system_event_context AS event_context,
  metrics.string.messaging_system_locale AS locale,
  metrics.text2.messaging_system_message_id AS message_id,
  metadata,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  client_info.app_channel AS release_channel,
  sample_id,
  metadata.user_agent.version AS version,
  metrics.uuid.messaging_system_browser_session_id AS browser_session_id,
  ping_info.experiments AS experiments,
  STRUCT(
    metrics.string.messaging_system_attribution_campaign AS campaign,
    metrics.string.messaging_system_attribution_content AS content,
    metrics.string.messaging_system_attribution_experiment AS experiment,
    metrics.string.messaging_system_attribution_medium AS medium,
    metrics.string.messaging_system_attribution_source AS source,
    metrics.string.messaging_system_attribution_ua AS ua,
    metrics.string.messaging_system_attribution_variation AS variation,
    metrics.string.messaging_system_attribution_dltoken AS dltoken,
    metrics.string.messaging_system_attribution_dlsource AS dlsource
  ) AS attribution
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.messaging_system_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND metrics.string.messaging_system_ping_type IS NULL
