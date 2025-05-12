-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.fx_suggest`
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
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.fx_suggest_block_id,
      metrics.quantity.fx_suggest_position
    ) AS `quantity`,
    STRUCT(
      metrics.string.fx_suggest_advertiser,
      metrics.string.fx_suggest_iab_category,
      metrics.string.fx_suggest_ping_type,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.url2.fx_suggest_reporting_url) AS `url2`,
    STRUCT(metrics.uuid.fx_suggest_context_id) AS `uuid`,
    STRUCT(metrics.boolean.fx_suggest_is_clicked) AS `boolean`,
    STRUCT(metrics.url.fx_suggest_reporting_url) AS `url`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefox.fx_suggest`
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
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.fx_suggest_block_id,
      metrics.quantity.fx_suggest_position
    ) AS `quantity`,
    STRUCT(
      metrics.string.fx_suggest_advertiser,
      metrics.string.fx_suggest_iab_category,
      metrics.string.fx_suggest_ping_type,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.url2.fx_suggest_reporting_url) AS `url2`,
    STRUCT(metrics.uuid.fx_suggest_context_id) AS `uuid`,
    STRUCT(metrics.boolean.fx_suggest_is_clicked) AS `boolean`,
    STRUCT(metrics.url.fx_suggest_reporting_url) AS `url`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.fx_suggest`
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
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.quantity.fx_suggest_block_id,
      metrics.quantity.fx_suggest_position
    ) AS `quantity`,
    STRUCT(
      metrics.string.fx_suggest_advertiser,
      metrics.string.fx_suggest_iab_category,
      metrics.string.fx_suggest_ping_type,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(metrics.url2.fx_suggest_reporting_url) AS `url2`,
    STRUCT(metrics.uuid.fx_suggest_context_id) AS `uuid`,
    STRUCT(metrics.boolean.fx_suggest_is_clicked) AS `boolean`,
    STRUCT(metrics.url.fx_suggest_reporting_url) AS `url`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_fennec.fx_suggest`
