SELECT
  DATE(submission_timestamp) AS submission_date,
  document_id,
  client_id,
  sample_id,
  payload.info.subsession_start_date,
  normalized_channel,
  key AS addon_id,
  value.blocklisted,
  value.name,
  value.user_disabled > 0 AS user_disabled,
  value.app_disabled,
  value.version,
  value.scope,
  value.type,
  value.foreign_install > 0 AS foreign_install,
  value.has_binary_components,
  value.install_day,
  value.update_day,
  value.signed_state,
  value.is_system,
  value.is_web_extension,
  value.multiprocess_compatible
FROM
  telemetry.main,
  UNNEST(
    IF(
      ARRAY_LENGTH(environment.addons.active_addons) > 0,
      environment.addons.active_addons,
      -- include a null addon if there were none (either null or an empty list)
      [environment.addons.active_addons[SAFE_OFFSET(0)]]
    )
  )
WHERE
  client_id IS NOT NULL
  AND normalized_app_name = 'Firefox'
  AND DATE(submission_timestamp) = @submission_date
