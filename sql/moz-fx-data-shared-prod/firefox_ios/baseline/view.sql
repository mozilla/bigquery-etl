-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.baseline`
AS
SELECT
  "org_mozilla_ios_firefox" AS normalized_app_id,
  "release" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.tabs_normal_and_private_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.search_counts,
      metrics.labeled_counter.search_in_content
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.search_default_engine,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(
      metrics.datetime.app_last_opened_as_default_browser,
      metrics.datetime.raw_app_last_opened_as_default_browser,
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
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
  app_version_patch
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.baseline`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxbeta" AS normalized_app_id,
  "beta" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.tabs_normal_and_private_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.search_counts,
      metrics.labeled_counter.search_in_content
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.search_default_engine,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(
      metrics.datetime.app_last_opened_as_default_browser,
      metrics.datetime.raw_app_last_opened_as_default_browser,
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
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
  app_version_patch
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.baseline`
UNION ALL
SELECT
  "org_mozilla_ios_fennec" AS normalized_app_id,
  "nightly" AS normalized_channel,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.counter.glean_validation_metrics_ping_count,
      metrics.counter.tabs_normal_and_private_uri_count
    ) AS `counter`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.glean_validation_pings_submitted,
      metrics.labeled_counter.browser_search_ad_clicks,
      metrics.labeled_counter.browser_search_with_ads,
      metrics.labeled_counter.search_counts,
      metrics.labeled_counter.search_in_content
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.glean_baseline_locale,
      metrics.string.search_default_engine,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.timespan.glean_baseline_duration) AS `timespan`,
    STRUCT(
      metrics.datetime.app_last_opened_as_default_browser,
      metrics.datetime.raw_app_last_opened_as_default_browser,
      metrics.datetime.glean_validation_first_run_hour,
      metrics.datetime.raw_glean_validation_first_run_hour
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
  app_version_patch
FROM
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.baseline`
