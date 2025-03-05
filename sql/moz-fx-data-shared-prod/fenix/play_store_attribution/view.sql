-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.play_store_attribution`
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_response_code,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term
    ) AS `string`,
    STRUCT(metrics.text2.play_store_attribution_install_referrer_response) AS `text2`,
    STRUCT(metrics.text.play_store_attribution_install_referrer_response) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.play_store_attribution`
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_response_code,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term
    ) AS `string`,
    STRUCT(metrics.text2.play_store_attribution_install_referrer_response) AS `text2`,
    STRUCT(metrics.text.play_store_attribution_install_referrer_response) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.play_store_attribution`
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_response_code,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term
    ) AS `string`,
    STRUCT(metrics.text2.play_store_attribution_install_referrer_response) AS `text2`,
    STRUCT(metrics.text.play_store_attribution_install_referrer_response) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.play_store_attribution`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_response_code,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term
    ) AS `string`,
    STRUCT(metrics.text2.play_store_attribution_install_referrer_response) AS `text2`,
    STRUCT(metrics.text.play_store_attribution_install_referrer_response) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.play_store_attribution`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
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
      metrics.string.glean_client_annotation_experimentation_id,
      metrics.string.play_store_attribution_campaign,
      metrics.string.play_store_attribution_content,
      metrics.string.play_store_attribution_medium,
      metrics.string.play_store_attribution_response_code,
      metrics.string.play_store_attribution_source,
      metrics.string.play_store_attribution_term
    ) AS `string`,
    STRUCT(metrics.text2.play_store_attribution_install_referrer_response) AS `text2`,
    STRUCT(metrics.text.play_store_attribution_install_referrer_response) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.play_store_attribution`
