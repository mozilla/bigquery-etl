-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.usage_reporting`
AS
SELECT
  "org_mozilla_ios_firefox" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timing_distribution.usage_duration) AS `timing_distribution`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`,
    STRUCT(metrics.boolean.usage_is_managed_device) AS `boolean`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.usage_reporting`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  "beta" AS normalized_channel,
  additional_properties,
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timing_distribution.usage_duration) AS `timing_distribution`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`,
    STRUCT(metrics.boolean.usage_is_managed_device) AS `boolean`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.usage_reporting`
UNION ALL
SELECT
  "org_mozilla_ios_fennec" AS normalized_app_id,
  "nightly" AS normalized_channel,
  additional_properties,
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.usage_app_build,
      metrics.string.usage_app_channel,
      metrics.string.usage_app_display_version,
      metrics.string.usage_os,
      metrics.string.usage_os_version,
      metrics.string.usage_reason
    ) AS `string`,
    STRUCT(metrics.uuid.usage_profile_id) AS `uuid`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timing_distribution.usage_duration) AS `timing_distribution`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`,
    STRUCT(metrics.boolean.usage_is_managed_device) AS `boolean`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.usage_reporting`
