SELECT
  DATE(submission_timestamp) AS submission_date,
  fxa.metrics.uuid.client_association_legacy_client_id AS legacy_telemetry_client_id,
  fxa.client_info,
  fxa.metadata,
  fxa.metrics.labeled_counter,
  fxa.metrics.string.glean_client_annotation_experimentation_id,
  fxa.ping_info.experiments,
  fxa.normalized_channel,
  fxa.normalized_app_name,
  fxa.app_version_major,
  fxa.app_version_minor,
  fxa.app_version_patch,
  fxa.normalized_os,
  fxa.normalized_os_version,
  fxa.normalized_country_code,
  fxa.sample_id
FROM
  `moz-fx-data-shared-prod.firefox_desktop.fx_accounts` fxa
WHERE
  DATE(submission_timestamp) = @submission_date
  AND fxa.client_info.client_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY fxa.client_info.client_id ORDER BY submission_timestamp DESC) = 1
