-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.usage_reporting`
AS
SELECT
  "org_mozilla_klar" AS normalized_app_id,
  normalized_channel,
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
    STRUCT(metrics.boolean.browser_is_default) AS `boolean`,
    STRUCT(metrics.datetime.usage_first_run_date) AS `datetime`,
    STRUCT(metrics.timespan.usage_duration) AS `timespan`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  sample_id,
  submission_timestamp,
  is_bot_generated
FROM
  `moz-fx-data-shared-prod.org_mozilla_klar.usage_reporting`
