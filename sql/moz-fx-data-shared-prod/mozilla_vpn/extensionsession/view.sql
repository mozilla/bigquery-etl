-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_vpn.extensionsession`
AS
SELECT
  "mozillavpn" AS normalized_app_id,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(
      metrics.boolean.extension_has_completed_onboarding,
      metrics.boolean.extension_used_feature_disable_firefox_protection,
      metrics.boolean.extension_used_feature_page_action_revoke_exclude,
      metrics.boolean.extension_used_feature_page_action_revoke_geopref,
      metrics.boolean.extension_used_feature_settings_page
    ) AS `boolean`,
    STRUCT(
      metrics.quantity.extension_count_excluded,
      metrics.quantity.extension_count_geoprefed
    ) AS `quantity`
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
  `moz-fx-data-shared-prod.mozillavpn.extensionsession`
UNION ALL
SELECT
  "org_mozilla_firefox_vpn" AS normalized_app_id,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(
      metrics.boolean.extension_has_completed_onboarding,
      metrics.boolean.extension_used_feature_disable_firefox_protection,
      metrics.boolean.extension_used_feature_page_action_revoke_exclude,
      metrics.boolean.extension_used_feature_page_action_revoke_geopref,
      metrics.boolean.extension_used_feature_settings_page
    ) AS `boolean`,
    STRUCT(
      metrics.quantity.extension_count_excluded,
      metrics.quantity.extension_count_geoprefed
    ) AS `quantity`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_vpn.extensionsession`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn" AS normalized_app_id,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(
      metrics.boolean.extension_has_completed_onboarding,
      metrics.boolean.extension_used_feature_disable_firefox_protection,
      metrics.boolean.extension_used_feature_page_action_revoke_exclude,
      metrics.boolean.extension_used_feature_page_action_revoke_geopref,
      metrics.boolean.extension_used_feature_settings_page
    ) AS `boolean`,
    STRUCT(
      metrics.quantity.extension_count_excluded,
      metrics.quantity.extension_count_geoprefed
    ) AS `quantity`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn.extensionsession`
UNION ALL
SELECT
  "org_mozilla_ios_firefoxvpn_network_extension" AS normalized_app_id,
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
    STRUCT(metrics.string.glean_client_annotation_experimentation_id) AS `string`,
    STRUCT(
      metrics.boolean.extension_has_completed_onboarding,
      metrics.boolean.extension_used_feature_disable_firefox_protection,
      metrics.boolean.extension_used_feature_page_action_revoke_exclude,
      metrics.boolean.extension_used_feature_page_action_revoke_geopref,
      metrics.boolean.extension_used_feature_settings_page
    ) AS `boolean`,
    STRUCT(
      metrics.quantity.extension_count_excluded,
      metrics.quantity.extension_count_geoprefed
    ) AS `quantity`
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
  `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension.extensionsession`
