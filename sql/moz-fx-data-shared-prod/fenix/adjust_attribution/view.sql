-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.adjust_attribution`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox",
    client_info.app_build
  ).channel AS normalized_channel,
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
      metrics.string.adjust_attribution_adgroup,
      metrics.string.adjust_attribution_campaign,
      metrics.string.adjust_attribution_creative,
      metrics.string.adjust_attribution_network,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.adjust_attribution_adjust_attribution_time
    ) AS `timing_distribution`,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.adjust_attribution`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
    client_info.app_build
  ).channel AS normalized_channel,
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
      metrics.string.adjust_attribution_adgroup,
      metrics.string.adjust_attribution_campaign,
      metrics.string.adjust_attribution_creative,
      metrics.string.adjust_attribution_network,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.adjust_attribution_adjust_attribution_time
    ) AS `timing_distribution`,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.adjust_attribution`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
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
      metrics.string.adjust_attribution_adgroup,
      metrics.string.adjust_attribution_campaign,
      metrics.string.adjust_attribution_creative,
      metrics.string.adjust_attribution_network,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timing_distribution.adjust_attribution_adjust_attribution_time
    ) AS `timing_distribution`,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.adjust_attribution`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  STRUCT(
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    client_info.build_date,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.session_count,
    client_info.session_id,
    client_info.telemetry_sdk_build,
    client_info.windows_build_number,
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
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
      metrics.string.adjust_attribution_adgroup,
      metrics.string.adjust_attribution_campaign,
      metrics.string.adjust_attribution_creative,
      metrics.string.adjust_attribution_network,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      STRUCT(
        CAST(NULL AS INTEGER) AS `bucket_count`,
        CAST(NULL AS INTEGER) AS `count`,
        CAST(NULL AS STRING) AS `histogram_type`,
        CAST(NULL AS INTEGER) AS `overflow`,
        CAST(NULL AS ARRAY<FLOAT64>) AS `range`,
        metrics.timing_distribution.adjust_attribution_adjust_attribution_time.sum,
        CAST(NULL AS STRING) AS `time_unit`,
        CAST(NULL AS INTEGER) AS `underflow`,
        metrics.timing_distribution.adjust_attribution_adjust_attribution_time.values
      ) AS `adjust_attribution_adjust_attribution_time`
    ) AS `timing_distribution`,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.adjust_attribution`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
    client_info.app_build
  ).channel AS normalized_channel,
  additional_properties,
  STRUCT(
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    client_info.build_date,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.session_count,
    client_info.session_id,
    client_info.telemetry_sdk_build,
    client_info.windows_build_number,
    client_info.attribution,
    client_info.distribution
  ) AS `client_info`,
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
      metrics.string.adjust_attribution_adgroup,
      metrics.string.adjust_attribution_campaign,
      metrics.string.adjust_attribution_creative,
      metrics.string.adjust_attribution_network,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      STRUCT(
        CAST(NULL AS INTEGER) AS `bucket_count`,
        CAST(NULL AS INTEGER) AS `count`,
        CAST(NULL AS STRING) AS `histogram_type`,
        CAST(NULL AS INTEGER) AS `overflow`,
        CAST(NULL AS ARRAY<FLOAT64>) AS `range`,
        metrics.timing_distribution.adjust_attribution_adjust_attribution_time.sum,
        CAST(NULL AS STRING) AS `time_unit`,
        CAST(NULL AS INTEGER) AS `underflow`,
        metrics.timing_distribution.adjust_attribution_adjust_attribution_time.values
      ) AS `adjust_attribution_adjust_attribution_time`
    ) AS `timing_distribution`,
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.adjust_attribution`
