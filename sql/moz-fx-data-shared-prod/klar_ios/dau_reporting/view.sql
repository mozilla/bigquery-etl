-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_ios.dau_reporting`
AS
SELECT
  "org_mozilla_ios_klar" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_in_content,
      metrics.labeled_counter.browser_search_search_count,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(metrics.string_list.glean_ping_uploader_capabilities) AS `string_list`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_klar.dau_reporting`
