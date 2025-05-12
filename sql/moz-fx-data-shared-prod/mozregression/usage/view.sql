-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozregression.usage`
AS
SELECT
  "org_mozilla_mozregression" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.usage_app,
      metrics.string.usage_variant,
      metrics.string.usage_build_type,
      metrics.string.usage_linux_distro,
      metrics.string.usage_linux_version,
      metrics.string.usage_mac_version,
      metrics.string.usage_python_version,
      metrics.string.usage_windows_version,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.datetime.usage_bad_date,
      metrics.datetime.raw_usage_bad_date,
      metrics.datetime.usage_good_date,
      metrics.datetime.raw_usage_good_date,
      metrics.datetime.usage_launch_date,
      metrics.datetime.raw_usage_launch_date
    ) AS `datetime`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  ping_info,
  sample_id,
  submission_timestamp,
  app_version_major,
  app_version_minor,
  app_version_patch,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_mozregression.usage`
