-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.fx_suggest`
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
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.fx_suggest`
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
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.fx_suggest`
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
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix.fx_suggest`
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
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.fx_suggest`
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
  submission_timestamp
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.fx_suggest`