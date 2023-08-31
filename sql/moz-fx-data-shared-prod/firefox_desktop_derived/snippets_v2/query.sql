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
  ping_info.experiments,
  metrics.string.messaging_system_source AS source
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.messaging_system_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND metrics.string.messaging_system_ping_type = 'snippets'
