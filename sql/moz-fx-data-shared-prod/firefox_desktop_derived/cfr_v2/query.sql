SELECT
  submission_timestamp,
  additional_properties,
  metrics.string.messaging_system_addon_version,
  metrics.string.messaging_system_bucket_id,
  metrics.uuid.messaging_system_client_id,
  document_id,
  metrics.string.messaging_system_event,
  metrics.text2.messaging_system_event_context,
  metrics.uuid.messaging_system_impression_id,
  metrics.string.messaging_system_locale,
  metrics.text2.messaging_system_message_id,
  metadata,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  client_info.app_channel,
  sample_id,
  metadata.user_agent.version,
  ping_info.experiments,
  metrics.string.messaging_system_source
FROM
  `moz-fx-data-shared-prod.firefox_desktop_stable.messaging_system_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND metrics.string.messaging_system_ping_type = 'cfr'
