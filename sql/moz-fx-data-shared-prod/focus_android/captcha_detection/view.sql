-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.captcha_detection`
AS
SELECT
  "org_mozilla_focus" AS normalized_app_id,
  "release" AS normalized_channel,
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
      metrics.counter.captcha_detection_pages_visited_pbm
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
    ) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_focus.captcha_detection`
UNION ALL
SELECT
  "org_mozilla_focus_beta" AS normalized_app_id,
  "beta" AS normalized_channel,
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
      metrics.counter.captcha_detection_pages_visited_pbm
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
    ) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_focus_beta.captcha_detection`
UNION ALL
SELECT
  "org_mozilla_focus_nightly" AS normalized_app_id,
  "nightly" AS normalized_channel,
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
      metrics.counter.captcha_detection_pages_visited_pbm
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
    ) AS `string`
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
  `moz-fx-data-shared-prod.org_mozilla_focus_nightly.captcha_detection`
