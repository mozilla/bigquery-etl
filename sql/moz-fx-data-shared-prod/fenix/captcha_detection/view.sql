-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.captcha_detection`
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
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning,
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning_pbm,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection_pbm,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting_pbmode,
      metrics.boolean.captcha_detection_privacy_trackingprotection_cryptomining_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_fingerprinting_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_pbm_enabled
    ) AS `boolean`,
    STRUCT(
      metrics.counter.captcha_detection_arkoselabs_pc,
      metrics.counter.captcha_detection_arkoselabs_pc_pbm,
      metrics.counter.captcha_detection_arkoselabs_pf,
      metrics.counter.captcha_detection_arkoselabs_pf_pbm,
      metrics.counter.captcha_detection_arkoselabs_ps,
      metrics.counter.captcha_detection_arkoselabs_ps_pbm,
      metrics.counter.captcha_detection_awswaf_pc,
      metrics.counter.captcha_detection_awswaf_pc_pbm,
      metrics.counter.captcha_detection_awswaf_pf,
      metrics.counter.captcha_detection_awswaf_pf_pbm,
      metrics.counter.captcha_detection_awswaf_ps,
      metrics.counter.captcha_detection_awswaf_ps_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf_pbm,
      metrics.counter.captcha_detection_datadome_bl,
      metrics.counter.captcha_detection_datadome_bl_pbm,
      metrics.counter.captcha_detection_datadome_pc,
      metrics.counter.captcha_detection_datadome_pc_pbm,
      metrics.counter.captcha_detection_datadome_ps,
      metrics.counter.captcha_detection_datadome_ps_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps_pbm,
      metrics.counter.captcha_detection_hcaptcha_ac,
      metrics.counter.captcha_detection_hcaptcha_ac_pbm,
      metrics.counter.captcha_detection_hcaptcha_pc,
      metrics.counter.captcha_detection_hcaptcha_pc_pbm,
      metrics.counter.captcha_detection_hcaptcha_ps,
      metrics.counter.captcha_detection_hcaptcha_ps_pbm,
      metrics.counter.captcha_detection_pages_visited,
      metrics.counter.captcha_detection_pages_visited_pbm,
      metrics.counter.captcha_detection_arkoselabs_oc,
      metrics.counter.captcha_detection_arkoselabs_oc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc_pbm,
      metrics.counter.captcha_detection_datadome_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc_pbm,
      metrics.counter.captcha_detection_hcaptcha_oc,
      metrics.counter.captcha_detection_hcaptcha_oc_pbm
    ) AS `counter`,
    STRUCT(
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required,
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required_pbm,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required_pbm
    ) AS `custom_distribution`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.captcha_detection_network_cookie_cookiebehavior,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox.captcha_detection`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS normalized_app_id,
  mozfun.norm.fenix_app_info(
    "org_mozilla_firefox_beta",
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
    STRUCT(
      client_info.attribution.campaign,
      client_info.attribution.content,
      client_info.attribution.medium,
      client_info.attribution.source,
      client_info.attribution.term,
      client_info.attribution.ext
    ) AS `attribution`,
    STRUCT(client_info.distribution.name, client_info.distribution.ext) AS `distribution`
  ) AS `client_info`,
  document_id,
  events,
  STRUCT(
    STRUCT(
      metadata.geo.city,
      metadata.geo.country,
      metadata.geo.db_version,
      metadata.geo.subdivision1,
      metadata.geo.subdivision2
    ) AS `geo`,
    STRUCT(
      metadata.header.date,
      metadata.header.dnt,
      metadata.header.x_debug_id,
      metadata.header.x_foxsec_ip_reputation,
      metadata.header.x_lb_tags,
      metadata.header.x_pingsender_version,
      metadata.header.x_source_tags,
      metadata.header.x_telemetry_agent,
      metadata.header.parsed_date,
      metadata.header.parsed_x_source_tags,
      metadata.header.parsed_x_lb_tags
    ) AS `header`,
    STRUCT(metadata.isp.db_version, metadata.isp.name, metadata.isp.organization) AS `isp`,
    metadata.user_agent
  ) AS `metadata`,
  STRUCT(
    STRUCT(
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning,
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning_pbm,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection_pbm,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting_pbmode,
      metrics.boolean.captcha_detection_privacy_trackingprotection_cryptomining_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_fingerprinting_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_pbm_enabled
    ) AS `boolean`,
    STRUCT(
      metrics.counter.captcha_detection_arkoselabs_pc,
      metrics.counter.captcha_detection_arkoselabs_pc_pbm,
      metrics.counter.captcha_detection_arkoselabs_pf,
      metrics.counter.captcha_detection_arkoselabs_pf_pbm,
      metrics.counter.captcha_detection_arkoselabs_ps,
      metrics.counter.captcha_detection_arkoselabs_ps_pbm,
      metrics.counter.captcha_detection_awswaf_pc,
      metrics.counter.captcha_detection_awswaf_pc_pbm,
      metrics.counter.captcha_detection_awswaf_pf,
      metrics.counter.captcha_detection_awswaf_pf_pbm,
      metrics.counter.captcha_detection_awswaf_ps,
      metrics.counter.captcha_detection_awswaf_ps_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf_pbm,
      metrics.counter.captcha_detection_datadome_bl,
      metrics.counter.captcha_detection_datadome_bl_pbm,
      metrics.counter.captcha_detection_datadome_pc,
      metrics.counter.captcha_detection_datadome_pc_pbm,
      metrics.counter.captcha_detection_datadome_ps,
      metrics.counter.captcha_detection_datadome_ps_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps_pbm,
      metrics.counter.captcha_detection_hcaptcha_ac,
      metrics.counter.captcha_detection_hcaptcha_ac_pbm,
      metrics.counter.captcha_detection_hcaptcha_pc,
      metrics.counter.captcha_detection_hcaptcha_pc_pbm,
      metrics.counter.captcha_detection_hcaptcha_ps,
      metrics.counter.captcha_detection_hcaptcha_ps_pbm,
      metrics.counter.captcha_detection_pages_visited,
      metrics.counter.captcha_detection_pages_visited_pbm,
      metrics.counter.captcha_detection_arkoselabs_oc,
      metrics.counter.captcha_detection_arkoselabs_oc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc_pbm,
      metrics.counter.captcha_detection_datadome_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc_pbm,
      metrics.counter.captcha_detection_hcaptcha_oc,
      metrics.counter.captcha_detection_hcaptcha_oc_pbm
    ) AS `counter`,
    STRUCT(
      STRUCT(
        metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required.count,
        metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required.sum,
        metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required.values
      ) AS `captcha_detection_arkoselabs_solutions_required`,
      STRUCT(
        metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required_pbm.count,
        metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required_pbm.sum,
        metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required_pbm.values
      ) AS `captcha_detection_arkoselabs_solutions_required_pbm`,
      STRUCT(
        metrics.custom_distribution.captcha_detection_awswaf_solutions_required.count,
        metrics.custom_distribution.captcha_detection_awswaf_solutions_required.sum,
        metrics.custom_distribution.captcha_detection_awswaf_solutions_required.values
      ) AS `captcha_detection_awswaf_solutions_required`,
      STRUCT(
        metrics.custom_distribution.captcha_detection_awswaf_solutions_required_pbm.count,
        metrics.custom_distribution.captcha_detection_awswaf_solutions_required_pbm.sum,
        metrics.custom_distribution.captcha_detection_awswaf_solutions_required_pbm.values
      ) AS `captcha_detection_awswaf_solutions_required_pbm`
    ) AS `custom_distribution`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.captcha_detection_network_cookie_cookiebehavior,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta.captcha_detection`
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
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning,
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning_pbm,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection_pbm,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting_pbmode,
      metrics.boolean.captcha_detection_privacy_trackingprotection_cryptomining_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_fingerprinting_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_pbm_enabled
    ) AS `boolean`,
    STRUCT(
      metrics.counter.captcha_detection_arkoselabs_pc,
      metrics.counter.captcha_detection_arkoselabs_pc_pbm,
      metrics.counter.captcha_detection_arkoselabs_pf,
      metrics.counter.captcha_detection_arkoselabs_pf_pbm,
      metrics.counter.captcha_detection_arkoselabs_ps,
      metrics.counter.captcha_detection_arkoselabs_ps_pbm,
      metrics.counter.captcha_detection_awswaf_pc,
      metrics.counter.captcha_detection_awswaf_pc_pbm,
      metrics.counter.captcha_detection_awswaf_pf,
      metrics.counter.captcha_detection_awswaf_pf_pbm,
      metrics.counter.captcha_detection_awswaf_ps,
      metrics.counter.captcha_detection_awswaf_ps_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf_pbm,
      metrics.counter.captcha_detection_datadome_bl,
      metrics.counter.captcha_detection_datadome_bl_pbm,
      metrics.counter.captcha_detection_datadome_pc,
      metrics.counter.captcha_detection_datadome_pc_pbm,
      metrics.counter.captcha_detection_datadome_ps,
      metrics.counter.captcha_detection_datadome_ps_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps_pbm,
      metrics.counter.captcha_detection_hcaptcha_ac,
      metrics.counter.captcha_detection_hcaptcha_ac_pbm,
      metrics.counter.captcha_detection_hcaptcha_pc,
      metrics.counter.captcha_detection_hcaptcha_pc_pbm,
      metrics.counter.captcha_detection_hcaptcha_ps,
      metrics.counter.captcha_detection_hcaptcha_ps_pbm,
      metrics.counter.captcha_detection_pages_visited,
      metrics.counter.captcha_detection_pages_visited_pbm,
      metrics.counter.captcha_detection_arkoselabs_oc,
      metrics.counter.captcha_detection_arkoselabs_oc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc_pbm,
      metrics.counter.captcha_detection_datadome_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc_pbm,
      metrics.counter.captcha_detection_hcaptcha_oc,
      metrics.counter.captcha_detection_hcaptcha_oc_pbm
    ) AS `counter`,
    STRUCT(
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required,
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required_pbm,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required_pbm
    ) AS `custom_distribution`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.captcha_detection_network_cookie_cookiebehavior,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix.captcha_detection`
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
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning,
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning_pbm,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection_pbm,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting_pbmode,
      metrics.boolean.captcha_detection_privacy_trackingprotection_cryptomining_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_fingerprinting_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_pbm_enabled
    ) AS `boolean`,
    STRUCT(
      metrics.counter.captcha_detection_arkoselabs_pc,
      metrics.counter.captcha_detection_arkoselabs_pc_pbm,
      metrics.counter.captcha_detection_arkoselabs_pf,
      metrics.counter.captcha_detection_arkoselabs_pf_pbm,
      metrics.counter.captcha_detection_arkoselabs_ps,
      metrics.counter.captcha_detection_arkoselabs_ps_pbm,
      metrics.counter.captcha_detection_awswaf_pc,
      metrics.counter.captcha_detection_awswaf_pc_pbm,
      metrics.counter.captcha_detection_awswaf_pf,
      metrics.counter.captcha_detection_awswaf_pf_pbm,
      metrics.counter.captcha_detection_awswaf_ps,
      metrics.counter.captcha_detection_awswaf_ps_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf_pbm,
      metrics.counter.captcha_detection_datadome_bl,
      metrics.counter.captcha_detection_datadome_bl_pbm,
      metrics.counter.captcha_detection_datadome_pc,
      metrics.counter.captcha_detection_datadome_pc_pbm,
      metrics.counter.captcha_detection_datadome_ps,
      metrics.counter.captcha_detection_datadome_ps_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps_pbm,
      metrics.counter.captcha_detection_hcaptcha_ac,
      metrics.counter.captcha_detection_hcaptcha_ac_pbm,
      metrics.counter.captcha_detection_hcaptcha_pc,
      metrics.counter.captcha_detection_hcaptcha_pc_pbm,
      metrics.counter.captcha_detection_hcaptcha_ps,
      metrics.counter.captcha_detection_hcaptcha_ps_pbm,
      metrics.counter.captcha_detection_pages_visited,
      metrics.counter.captcha_detection_pages_visited_pbm,
      metrics.counter.captcha_detection_arkoselabs_oc,
      metrics.counter.captcha_detection_arkoselabs_oc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc_pbm,
      metrics.counter.captcha_detection_datadome_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc_pbm,
      metrics.counter.captcha_detection_hcaptcha_oc,
      metrics.counter.captcha_detection_hcaptcha_oc_pbm
    ) AS `counter`,
    STRUCT(
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required,
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required_pbm,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required_pbm
    ) AS `custom_distribution`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.captcha_detection_network_cookie_cookiebehavior,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly.captcha_detection`
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
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning,
      metrics.boolean.captcha_detection_network_cookie_cookiebehavior_optinpartitioning_pbm,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection,
      metrics.boolean.captcha_detection_privacy_fingerprintingprotection_pbm,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting,
      metrics.boolean.captcha_detection_privacy_resistfingerprinting_pbmode,
      metrics.boolean.captcha_detection_privacy_trackingprotection_cryptomining_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_fingerprinting_enabled,
      metrics.boolean.captcha_detection_privacy_trackingprotection_pbm_enabled
    ) AS `boolean`,
    STRUCT(
      metrics.counter.captcha_detection_arkoselabs_pc,
      metrics.counter.captcha_detection_arkoselabs_pc_pbm,
      metrics.counter.captcha_detection_arkoselabs_pf,
      metrics.counter.captcha_detection_arkoselabs_pf_pbm,
      metrics.counter.captcha_detection_arkoselabs_ps,
      metrics.counter.captcha_detection_arkoselabs_ps_pbm,
      metrics.counter.captcha_detection_awswaf_pc,
      metrics.counter.captcha_detection_awswaf_pc_pbm,
      metrics.counter.captcha_detection_awswaf_pf,
      metrics.counter.captcha_detection_awswaf_pf_pbm,
      metrics.counter.captcha_detection_awswaf_ps,
      metrics.counter.captcha_detection_awswaf_ps_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc,
      metrics.counter.captcha_detection_cloudflare_turnstile_cc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf,
      metrics.counter.captcha_detection_cloudflare_turnstile_cf_pbm,
      metrics.counter.captcha_detection_datadome_bl,
      metrics.counter.captcha_detection_datadome_bl_pbm,
      metrics.counter.captcha_detection_datadome_pc,
      metrics.counter.captcha_detection_datadome_pc_pbm,
      metrics.counter.captcha_detection_datadome_ps,
      metrics.counter.captcha_detection_datadome_ps_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac,
      metrics.counter.captcha_detection_google_recaptcha_v2_ac_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc,
      metrics.counter.captcha_detection_google_recaptcha_v2_pc_pbm,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps,
      metrics.counter.captcha_detection_google_recaptcha_v2_ps_pbm,
      metrics.counter.captcha_detection_hcaptcha_ac,
      metrics.counter.captcha_detection_hcaptcha_ac_pbm,
      metrics.counter.captcha_detection_hcaptcha_pc,
      metrics.counter.captcha_detection_hcaptcha_pc_pbm,
      metrics.counter.captcha_detection_hcaptcha_ps,
      metrics.counter.captcha_detection_hcaptcha_ps_pbm,
      metrics.counter.captcha_detection_pages_visited,
      metrics.counter.captcha_detection_pages_visited_pbm,
      metrics.counter.captcha_detection_arkoselabs_oc,
      metrics.counter.captcha_detection_arkoselabs_oc_pbm,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc,
      metrics.counter.captcha_detection_cloudflare_turnstile_oc_pbm,
      metrics.counter.captcha_detection_datadome_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc,
      metrics.counter.captcha_detection_google_recaptcha_v2_oc_pbm,
      metrics.counter.captcha_detection_hcaptcha_oc,
      metrics.counter.captcha_detection_hcaptcha_oc_pbm
    ) AS `counter`,
    STRUCT(
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required,
      metrics.custom_distribution.captcha_detection_arkoselabs_solutions_required_pbm,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required,
      metrics.custom_distribution.captcha_detection_awswaf_solutions_required_pbm
    ) AS `custom_distribution`,
    STRUCT(
      metrics.labeled_counter.glean_error_invalid_label,
      metrics.labeled_counter.glean_error_invalid_overflow,
      metrics.labeled_counter.glean_error_invalid_state,
      metrics.labeled_counter.glean_error_invalid_value
    ) AS `labeled_counter`,
    STRUCT(
      metrics.string.captcha_detection_network_cookie_cookiebehavior,
      metrics.string.glean_client_annotation_experimentation_id
    ) AS `string`,
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
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora.captcha_detection`
