-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.klar_android.baseline`
AS
SELECT
  "org_mozilla_klar" AS normalized_app_id,
  normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.browser_total_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_in_content,
      metrics.labeled_counter.browser_search_search_count
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.browser_default_search_engine,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(metrics.uuid.legacy_ids_client_id) AS `uuid`
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
  `moz-fx-data-shared-prod.org_mozilla_klar.baseline`
