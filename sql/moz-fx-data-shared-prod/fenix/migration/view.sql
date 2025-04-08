-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.migration`
AS
SELECT
  "org_mozilla_firefox" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox",
    client_info.app_build
  ).channel AS normalized_channel,
  submission_date,
  additional_properties,
  STRUCT(
    client_info.android_sdk_version,
    client_info.app_build,
    client_info.app_channel,
    client_info.app_display_version,
    client_info.architecture,
    client_info.client_id,
    client_info.device_manufacturer,
    client_info.device_model,
    client_info.first_run_date,
    client_info.locale,
    client_info.os,
    client_info.os_version,
    client_info.telemetry_sdk_build,
    client_info.build_date,
    client_info.windows_build_number,
    client_info.session_count,
    client_info.session_id,
    STRUCT(
      client_info.attribution.campaign,
      client_info.attribution.content,
      client_info.attribution.medium,
      client_info.attribution.source,
      client_info.attribution.term,
      CAST(NULL AS JSON) AS `ext`
    ) AS `attribution`,
    STRUCT(client_info.distribution.name, CAST(NULL AS JSON) AS `ext`) AS `distribution`
  ) AS `client_info`,
  document_id,
  events,
  STRUCT(
    metadata.geo,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      CAST(NULL AS TIMESTAMP) AS `parsed_date`,
      CAST(NULL AS ARRAY<STRING>) AS `parsed_x_source_tags`,
      CAST(NULL AS STRUCT<`tls_version` STRING, `tls_cipher_hex` STRING>) AS `parsed_x_lb_tags`
    ) AS `header`,
    metadata.user_agent,
    metadata.isp
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.boolean.migration_addons_any_failures,
      metrics.boolean.migration_bookmarks_any_failures,
      metrics.boolean.migration_fxa_any_failures,
      metrics.boolean.migration_fxa_has_custom_idp_server,
      metrics.boolean.migration_fxa_has_custom_token_server,
      metrics.boolean.migration_gecko_any_failures,
      metrics.boolean.migration_history_any_failures,
      metrics.boolean.migration_logins_any_failures,
      metrics.boolean.migration_open_tabs_any_failures,
      metrics.boolean.migration_settings_any_failures,
      metrics.boolean.migration_settings_telemetry_enabled,
      metrics.boolean.migration_telemetry_identifiers_any_failures,
      metrics.boolean.migration_search_any_failures,
      metrics.boolean.migration_pinned_sites_any_failures
    ) AS `boolean`,
    STRUCT(
      metrics.counter.migration_addons_failed_addons,
      metrics.counter.migration_addons_failure_reason,
      metrics.counter.migration_addons_migrated_addons,
      metrics.counter.migration_addons_success_reason,
      metrics.counter.migration_bookmarks_detected,
      metrics.counter.migration_fxa_failure_reason,
      metrics.counter.migration_fxa_success_reason,
      metrics.counter.migration_history_detected,
      metrics.counter.migration_logins_detected,
      metrics.counter.migration_logins_failure_reason,
      metrics.counter.migration_logins_success_reason,
      metrics.counter.migration_logins_unsupported_db_version,
      metrics.counter.migration_open_tabs_detected,
      metrics.counter.migration_open_tabs_migrated,
      metrics.counter.migration_settings_failure_reason,
      metrics.counter.migration_settings_success_reason,
      metrics.counter.migration_bookmarks_failure_reason,
      metrics.counter.migration_bookmarks_success_reason,
      metrics.counter.migration_gecko_failure_reason,
      metrics.counter.migration_gecko_success_reason,
      metrics.counter.migration_history_failure_reason,
      metrics.counter.migration_history_success_reason,
      metrics.counter.migration_open_tabs_failure_reason,
      metrics.counter.migration_open_tabs_success_reason,
      metrics.counter.migration_telemetry_identifiers_failure_reason,
      metrics.counter.migration_telemetry_identifiers_success_reason,
      metrics.counter.migration_search_failure_reason,
      metrics.counter.migration_search_success_reason,
      metrics.counter.migration_pinned_sites_detected_pinned_sites,
      metrics.counter.migration_pinned_sites_failure_reason,
      metrics.counter.migration_pinned_sites_migrated_pinned_sites,
      metrics.counter.migration_pinned_sites_success_reason
    ) AS `counter`,
    STRUCT(
      metrics.datetime.migration_telemetry_identifiers_fennec_profile_creation_date,
      CAST(NULL AS STRING) AS `raw_migration_telemetry_identifiers_fennec_profile_creation_date`
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.migration_bookmarks_migrated,
      metrics.labeled_counter.migration_history_migrated,
      metrics.labeled_counter.migration_logins_failure_counts
    ) AS `labeled_counter`,
    STRUCT(metrics.labeled_string.migration_migration_versions) AS `labeled_string`,
    STRUCT(
      metrics.string.migration_fxa_bad_auth_state,
      metrics.string.migration_fxa_failure_reason_rust,
      metrics.string.migration_fxa_unsupported_account_version,
      metrics.string.migration_fxa_unsupported_pickle_version,
      metrics.string.migration_fxa_unsupported_state_version,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timespan.migration_bookmarks_duration,
      metrics.timespan.migration_history_duration,
      metrics.timespan.migration_addons_total_duration,
      metrics.timespan.migration_bookmarks_total_duration,
      metrics.timespan.migration_fxa_total_duration,
      metrics.timespan.migration_gecko_total_duration,
      metrics.timespan.migration_history_total_duration,
      metrics.timespan.migration_logins_total_duration,
      metrics.timespan.migration_open_tabs_total_duration,
      metrics.timespan.migration_pinned_sites_total_duration,
      metrics.timespan.migration_search_total_duration,
      metrics.timespan.migration_settings_total_duration,
      metrics.timespan.migration_telemetry_identifiers_total_duration
    ) AS `timespan`,
    STRUCT(metrics.uuid.migration_telemetry_identifiers_fennec_client_id) AS `uuid`,
    ARRAY(SELECT STRUCT(jwe.key, jwe.value) FROM UNNEST(metrics.jwe) AS `jwe`) AS `jwe`,
    ARRAY(
      SELECT
        STRUCT(labeled_rate.key, labeled_rate.value)
      FROM
        UNNEST(metrics.labeled_rate) AS `labeled_rate`
    ) AS `labeled_rate`,
    ARRAY(SELECT STRUCT(url.key, url.value) FROM UNNEST(metrics.url) AS `url`) AS `url`,
    ARRAY(SELECT STRUCT(text.key, text.value) FROM UNNEST(metrics.text) AS `text`) AS `text`
  ) AS `metrics`,
  normalized_app_name,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  STRUCT(
    ping_info.end_time,
    ping_info.experiments,
    ping_info.ping_type,
    ping_info.reason,
    ping_info.seq,
    ping_info.start_time,
    CAST(NULL AS TIMESTAMP) AS `parsed_start_time`,
    CAST(NULL AS TIMESTAMP) AS `parsed_end_time`
  ) AS `ping_info`,
  sample_id,
  submission_timestamp,
  CAST(NULL AS NUMERIC) AS `app_version_major`,
  CAST(NULL AS NUMERIC) AS `app_version_minor`,
  CAST(NULL AS NUMERIC) AS `app_version_patch`
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox.migration`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.migration_addons_any_failures,
      metrics.boolean.migration_bookmarks_any_failures,
      metrics.boolean.migration_fxa_any_failures,
      metrics.boolean.migration_fxa_has_custom_idp_server,
      metrics.boolean.migration_fxa_has_custom_token_server,
      metrics.boolean.migration_gecko_any_failures,
      metrics.boolean.migration_history_any_failures,
      metrics.boolean.migration_logins_any_failures,
      metrics.boolean.migration_open_tabs_any_failures,
      metrics.boolean.migration_settings_any_failures,
      metrics.boolean.migration_settings_telemetry_enabled,
      metrics.boolean.migration_telemetry_identifiers_any_failures,
      metrics.boolean.migration_search_any_failures,
      metrics.boolean.migration_pinned_sites_any_failures
    ) AS `boolean`,
    STRUCT(
      metrics.counter.migration_addons_failed_addons,
      metrics.counter.migration_addons_failure_reason,
      metrics.counter.migration_addons_migrated_addons,
      metrics.counter.migration_addons_success_reason,
      metrics.counter.migration_bookmarks_detected,
      metrics.counter.migration_fxa_failure_reason,
      metrics.counter.migration_fxa_success_reason,
      metrics.counter.migration_history_detected,
      metrics.counter.migration_logins_detected,
      metrics.counter.migration_logins_failure_reason,
      metrics.counter.migration_logins_success_reason,
      metrics.counter.migration_logins_unsupported_db_version,
      metrics.counter.migration_open_tabs_detected,
      metrics.counter.migration_open_tabs_migrated,
      metrics.counter.migration_settings_failure_reason,
      metrics.counter.migration_settings_success_reason,
      metrics.counter.migration_bookmarks_failure_reason,
      metrics.counter.migration_bookmarks_success_reason,
      metrics.counter.migration_gecko_failure_reason,
      metrics.counter.migration_gecko_success_reason,
      metrics.counter.migration_history_failure_reason,
      metrics.counter.migration_history_success_reason,
      metrics.counter.migration_open_tabs_failure_reason,
      metrics.counter.migration_open_tabs_success_reason,
      metrics.counter.migration_telemetry_identifiers_failure_reason,
      metrics.counter.migration_telemetry_identifiers_success_reason,
      metrics.counter.migration_search_failure_reason,
      metrics.counter.migration_search_success_reason,
      metrics.counter.migration_pinned_sites_detected_pinned_sites,
      metrics.counter.migration_pinned_sites_failure_reason,
      metrics.counter.migration_pinned_sites_migrated_pinned_sites,
      metrics.counter.migration_pinned_sites_success_reason
    ) AS `counter`,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.migration_bookmarks_migrated,
      metrics.labeled_counter.migration_history_migrated,
      metrics.labeled_counter.migration_logins_failure_counts
    ) AS `labeled_counter`,
    STRUCT(metrics.labeled_string.migration_migration_versions) AS `labeled_string`,
    STRUCT(
      metrics.string.migration_fxa_bad_auth_state,
      metrics.string.migration_fxa_failure_reason_rust,
      metrics.string.migration_fxa_unsupported_account_version,
      metrics.string.migration_fxa_unsupported_pickle_version,
      metrics.string.migration_fxa_unsupported_state_version,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timespan.migration_bookmarks_duration,
      metrics.timespan.migration_history_duration,
      metrics.timespan.migration_addons_total_duration,
      metrics.timespan.migration_bookmarks_total_duration,
      metrics.timespan.migration_fxa_total_duration,
      metrics.timespan.migration_gecko_total_duration,
      metrics.timespan.migration_history_total_duration,
      metrics.timespan.migration_logins_total_duration,
      metrics.timespan.migration_open_tabs_total_duration,
      metrics.timespan.migration_pinned_sites_total_duration,
      metrics.timespan.migration_search_total_duration,
      metrics.timespan.migration_settings_total_duration,
      metrics.timespan.migration_telemetry_identifiers_total_duration
    ) AS `timespan`,
    STRUCT(metrics.uuid.migration_telemetry_identifiers_fennec_client_id) AS `uuid`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.migration`
UNION ALL
SELECT
  "org_mozilla_fenix" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.migration_addons_any_failures,
      metrics.boolean.migration_bookmarks_any_failures,
      metrics.boolean.migration_fxa_any_failures,
      metrics.boolean.migration_fxa_has_custom_idp_server,
      metrics.boolean.migration_fxa_has_custom_token_server,
      metrics.boolean.migration_gecko_any_failures,
      metrics.boolean.migration_history_any_failures,
      metrics.boolean.migration_logins_any_failures,
      metrics.boolean.migration_open_tabs_any_failures,
      metrics.boolean.migration_settings_any_failures,
      metrics.boolean.migration_settings_telemetry_enabled,
      metrics.boolean.migration_telemetry_identifiers_any_failures,
      metrics.boolean.migration_search_any_failures,
      metrics.boolean.migration_pinned_sites_any_failures
    ) AS `boolean`,
    STRUCT(
      metrics.counter.migration_addons_failed_addons,
      metrics.counter.migration_addons_failure_reason,
      metrics.counter.migration_addons_migrated_addons,
      metrics.counter.migration_addons_success_reason,
      metrics.counter.migration_bookmarks_detected,
      metrics.counter.migration_fxa_failure_reason,
      metrics.counter.migration_fxa_success_reason,
      metrics.counter.migration_history_detected,
      metrics.counter.migration_logins_detected,
      metrics.counter.migration_logins_failure_reason,
      metrics.counter.migration_logins_success_reason,
      metrics.counter.migration_logins_unsupported_db_version,
      metrics.counter.migration_open_tabs_detected,
      metrics.counter.migration_open_tabs_migrated,
      metrics.counter.migration_settings_failure_reason,
      metrics.counter.migration_settings_success_reason,
      metrics.counter.migration_bookmarks_failure_reason,
      metrics.counter.migration_bookmarks_success_reason,
      metrics.counter.migration_gecko_failure_reason,
      metrics.counter.migration_gecko_success_reason,
      metrics.counter.migration_history_failure_reason,
      metrics.counter.migration_history_success_reason,
      metrics.counter.migration_open_tabs_failure_reason,
      metrics.counter.migration_open_tabs_success_reason,
      metrics.counter.migration_telemetry_identifiers_failure_reason,
      metrics.counter.migration_telemetry_identifiers_success_reason,
      metrics.counter.migration_search_failure_reason,
      metrics.counter.migration_search_success_reason,
      metrics.counter.migration_pinned_sites_detected_pinned_sites,
      metrics.counter.migration_pinned_sites_failure_reason,
      metrics.counter.migration_pinned_sites_migrated_pinned_sites,
      metrics.counter.migration_pinned_sites_success_reason
    ) AS `counter`,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.migration_bookmarks_migrated,
      metrics.labeled_counter.migration_history_migrated,
      metrics.labeled_counter.migration_logins_failure_counts
    ) AS `labeled_counter`,
    STRUCT(metrics.labeled_string.migration_migration_versions) AS `labeled_string`,
    STRUCT(
      metrics.string.migration_fxa_bad_auth_state,
      metrics.string.migration_fxa_failure_reason_rust,
      metrics.string.migration_fxa_unsupported_account_version,
      metrics.string.migration_fxa_unsupported_pickle_version,
      metrics.string.migration_fxa_unsupported_state_version,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timespan.migration_bookmarks_duration,
      metrics.timespan.migration_history_duration,
      metrics.timespan.migration_addons_total_duration,
      metrics.timespan.migration_bookmarks_total_duration,
      metrics.timespan.migration_fxa_total_duration,
      metrics.timespan.migration_gecko_total_duration,
      metrics.timespan.migration_history_total_duration,
      metrics.timespan.migration_logins_total_duration,
      metrics.timespan.migration_open_tabs_total_duration,
      metrics.timespan.migration_pinned_sites_total_duration,
      metrics.timespan.migration_search_total_duration,
      metrics.timespan.migration_settings_total_duration,
      metrics.timespan.migration_telemetry_identifiers_total_duration
    ) AS `timespan`,
    STRUCT(metrics.uuid.migration_telemetry_identifiers_fennec_client_id) AS `uuid`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.migration`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fenix_nightly",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.migration_addons_any_failures,
      metrics.boolean.migration_bookmarks_any_failures,
      metrics.boolean.migration_fxa_any_failures,
      metrics.boolean.migration_fxa_has_custom_idp_server,
      metrics.boolean.migration_fxa_has_custom_token_server,
      metrics.boolean.migration_gecko_any_failures,
      metrics.boolean.migration_history_any_failures,
      metrics.boolean.migration_logins_any_failures,
      metrics.boolean.migration_open_tabs_any_failures,
      metrics.boolean.migration_settings_any_failures,
      metrics.boolean.migration_settings_telemetry_enabled,
      metrics.boolean.migration_telemetry_identifiers_any_failures,
      metrics.boolean.migration_search_any_failures,
      metrics.boolean.migration_pinned_sites_any_failures
    ) AS `boolean`,
    STRUCT(
      metrics.counter.migration_addons_failed_addons,
      metrics.counter.migration_addons_failure_reason,
      metrics.counter.migration_addons_migrated_addons,
      metrics.counter.migration_addons_success_reason,
      metrics.counter.migration_bookmarks_detected,
      metrics.counter.migration_fxa_failure_reason,
      metrics.counter.migration_fxa_success_reason,
      metrics.counter.migration_history_detected,
      metrics.counter.migration_logins_detected,
      metrics.counter.migration_logins_failure_reason,
      metrics.counter.migration_logins_success_reason,
      metrics.counter.migration_logins_unsupported_db_version,
      metrics.counter.migration_open_tabs_detected,
      metrics.counter.migration_open_tabs_migrated,
      metrics.counter.migration_settings_failure_reason,
      metrics.counter.migration_settings_success_reason,
      metrics.counter.migration_bookmarks_failure_reason,
      metrics.counter.migration_bookmarks_success_reason,
      metrics.counter.migration_gecko_failure_reason,
      metrics.counter.migration_gecko_success_reason,
      metrics.counter.migration_history_failure_reason,
      metrics.counter.migration_history_success_reason,
      metrics.counter.migration_open_tabs_failure_reason,
      metrics.counter.migration_open_tabs_success_reason,
      metrics.counter.migration_telemetry_identifiers_failure_reason,
      metrics.counter.migration_telemetry_identifiers_success_reason,
      metrics.counter.migration_search_failure_reason,
      metrics.counter.migration_search_success_reason,
      metrics.counter.migration_pinned_sites_detected_pinned_sites,
      metrics.counter.migration_pinned_sites_failure_reason,
      metrics.counter.migration_pinned_sites_migrated_pinned_sites,
      metrics.counter.migration_pinned_sites_success_reason
    ) AS `counter`,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.migration_bookmarks_migrated,
      metrics.labeled_counter.migration_history_migrated,
      metrics.labeled_counter.migration_logins_failure_counts
    ) AS `labeled_counter`,
    STRUCT(metrics.labeled_string.migration_migration_versions) AS `labeled_string`,
    STRUCT(
      metrics.string.migration_fxa_bad_auth_state,
      metrics.string.migration_fxa_failure_reason_rust,
      metrics.string.migration_fxa_unsupported_account_version,
      metrics.string.migration_fxa_unsupported_pickle_version,
      metrics.string.migration_fxa_unsupported_state_version,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timespan.migration_bookmarks_duration,
      metrics.timespan.migration_history_duration,
      metrics.timespan.migration_addons_total_duration,
      metrics.timespan.migration_bookmarks_total_duration,
      metrics.timespan.migration_fxa_total_duration,
      metrics.timespan.migration_gecko_total_duration,
      metrics.timespan.migration_history_total_duration,
      metrics.timespan.migration_logins_total_duration,
      metrics.timespan.migration_open_tabs_total_duration,
      metrics.timespan.migration_pinned_sites_total_duration,
      metrics.timespan.migration_search_total_duration,
      metrics.timespan.migration_settings_total_duration,
      metrics.timespan.migration_telemetry_identifiers_total_duration
    ) AS `timespan`,
    STRUCT(metrics.uuid.migration_telemetry_identifiers_fennec_client_id) AS `uuid`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.migration`
UNION ALL
SELECT
  "org_mozilla_fennec_aurora" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_fennec_aurora",
    client_info.app_build
  ).channel AS normalized_channel,
  CAST(NULL AS DATE) AS `submission_date`,
  additional_properties,
  client_info,
  document_id,
  events,
  metadata,
  STRUCT(
    STRUCT(
      metrics.boolean.migration_addons_any_failures,
      metrics.boolean.migration_bookmarks_any_failures,
      metrics.boolean.migration_fxa_any_failures,
      metrics.boolean.migration_fxa_has_custom_idp_server,
      metrics.boolean.migration_fxa_has_custom_token_server,
      metrics.boolean.migration_gecko_any_failures,
      metrics.boolean.migration_history_any_failures,
      metrics.boolean.migration_logins_any_failures,
      metrics.boolean.migration_open_tabs_any_failures,
      metrics.boolean.migration_settings_any_failures,
      metrics.boolean.migration_settings_telemetry_enabled,
      metrics.boolean.migration_telemetry_identifiers_any_failures,
      metrics.boolean.migration_search_any_failures,
      metrics.boolean.migration_pinned_sites_any_failures
    ) AS `boolean`,
    STRUCT(
      metrics.counter.migration_addons_failed_addons,
      metrics.counter.migration_addons_failure_reason,
      metrics.counter.migration_addons_migrated_addons,
      metrics.counter.migration_addons_success_reason,
      metrics.counter.migration_bookmarks_detected,
      metrics.counter.migration_fxa_failure_reason,
      metrics.counter.migration_fxa_success_reason,
      metrics.counter.migration_history_detected,
      metrics.counter.migration_logins_detected,
      metrics.counter.migration_logins_failure_reason,
      metrics.counter.migration_logins_success_reason,
      metrics.counter.migration_logins_unsupported_db_version,
      metrics.counter.migration_open_tabs_detected,
      metrics.counter.migration_open_tabs_migrated,
      metrics.counter.migration_settings_failure_reason,
      metrics.counter.migration_settings_success_reason,
      metrics.counter.migration_bookmarks_failure_reason,
      metrics.counter.migration_bookmarks_success_reason,
      metrics.counter.migration_gecko_failure_reason,
      metrics.counter.migration_gecko_success_reason,
      metrics.counter.migration_history_failure_reason,
      metrics.counter.migration_history_success_reason,
      metrics.counter.migration_open_tabs_failure_reason,
      metrics.counter.migration_open_tabs_success_reason,
      metrics.counter.migration_telemetry_identifiers_failure_reason,
      metrics.counter.migration_telemetry_identifiers_success_reason,
      metrics.counter.migration_search_failure_reason,
      metrics.counter.migration_search_success_reason,
      metrics.counter.migration_pinned_sites_detected_pinned_sites,
      metrics.counter.migration_pinned_sites_failure_reason,
      metrics.counter.migration_pinned_sites_migrated_pinned_sites,
      metrics.counter.migration_pinned_sites_success_reason
    ) AS `counter`,
    STRUCT(
      CAST(NULL AS STRING) AS `migration_telemetry_identifiers_fennec_profile_creation_date`,
      metrics.datetime.raw_migration_telemetry_identifiers_fennec_profile_creation_date
    ) AS `datetime`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value,
      metrics.labeled_counter.migration_bookmarks_migrated,
      metrics.labeled_counter.migration_history_migrated,
      metrics.labeled_counter.migration_logins_failure_counts
    ) AS `labeled_counter`,
    STRUCT(metrics.labeled_string.migration_migration_versions) AS `labeled_string`,
    STRUCT(
      metrics.string.migration_fxa_bad_auth_state,
      metrics.string.migration_fxa_failure_reason_rust,
      metrics.string.migration_fxa_unsupported_account_version,
      metrics.string.migration_fxa_unsupported_pickle_version,
      metrics.string.migration_fxa_unsupported_state_version,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
    STRUCT(
      metrics.timespan.migration_bookmarks_duration,
      metrics.timespan.migration_history_duration,
      metrics.timespan.migration_addons_total_duration,
      metrics.timespan.migration_bookmarks_total_duration,
      metrics.timespan.migration_fxa_total_duration,
      metrics.timespan.migration_gecko_total_duration,
      metrics.timespan.migration_history_total_duration,
      metrics.timespan.migration_logins_total_duration,
      metrics.timespan.migration_open_tabs_total_duration,
      metrics.timespan.migration_pinned_sites_total_duration,
      metrics.timespan.migration_search_total_duration,
      metrics.timespan.migration_settings_total_duration,
      metrics.timespan.migration_telemetry_identifiers_total_duration
    ) AS `timespan`,
    STRUCT(metrics.uuid.migration_telemetry_identifiers_fennec_client_id) AS `uuid`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `jwe`,
    CAST(
      NULL
      AS
        ARRAY<
          STRUCT<
            `key` STRING,
            `value` ARRAY<
              STRUCT<`key` STRING, `value` STRUCT<`denominator` INTEGER, `numerator` INTEGER>>
            >
          >
        >
    ) AS `labeled_rate`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `url`,
    CAST(NULL AS ARRAY<STRUCT<`key` STRING, `value` STRING>>) AS `text`
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.migration`
