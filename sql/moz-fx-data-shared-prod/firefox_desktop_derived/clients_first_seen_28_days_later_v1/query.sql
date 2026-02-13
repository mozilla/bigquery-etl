WITH clients_first_seen_28_days_ago AS (
  SELECT
    client_id,
    sample_id,
    baseline_first_seen_date AS first_seen_date,
    baseline_architecture AS architecture,
    baseline_normalized_app_name AS app_name,
    baseline_locale AS locale,
    baseline_app_display_version AS app_version,
    metric_xpcom_abi AS xpcom_abi,
    baseline_distribution_id AS distribution_id,
    baseline_distribution_version AS partner_distribution_version,
    baseline_distributor AS partner_distributor,
    baseline_distributor_channel AS partner_distributor_channel,
    baseline_distribution_partner_id AS partner_id,
    baseline_attribution.campaign AS attribution_campaign,
    baseline_attribution.content AS attribution_content,
    baseline_attribution_msclkid AS attribution_msclkid,
    baseline_attribution_dltoken AS attribution_dltoken,
    baseline_attribution_dlsource AS attribution_dlsource,
    baseline_attribution_experiment AS attribution_experiment,
    baseline_attribution.medium AS attribution_medium,
    baseline_attribution.source AS attribution_source,
    baseline_attribution_ua AS attribution_ua,
    baseline_attribution_variation AS attribution_variation,
    metric_apple_model_id AS apple_model_id,
    baseline_isp AS isp_name,
    baseline_normalized_channel AS normalized_channel,
    baseline_country AS country,
    baseline_normalized_os AS normalized_os,
    baseline_normalized_os_version AS normalized_os_version,
    baseline_startup_profile_selection_reason_first AS startup_profile_selection_reason,
    metric_installation_first_seen_admin_user AS installation_first_seen_admin_user,
    metric_installation_first_seen_default_path AS installation_first_seen_default_path,
    metric_installation_first_seen_failure_reason AS installation_first_seen_failure_reason,
    metric_installation_first_seen_from_msi AS installation_first_seen_from_msi,
    metric_installation_first_seen_install_existed AS installation_first_seen_install_existed,
    metric_installation_first_seen_installer_type AS installation_first_seen_installer_type,
    metric_installation_first_seen_other_inst AS installation_first_seen_other_inst,
    metric_installation_first_seen_other_msix_inst AS installation_first_seen_other_msix_inst,
    metric_installation_first_seen_profdir_existed AS installation_first_seen_profdir_existed,
    metric_installation_first_seen_silent AS installation_first_seen_silent,
    metric_installation_first_seen_version AS installation_first_seen_version,
    baseline_windows_build_number AS windows_build_number,
    baseline_windows_version AS windows_version,
    baseline_legacy_telemetry_profile_group_id AS profile_group_id,
    baseline_app_build_id AS app_build_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.clients_first_seen`
  WHERE
    baseline_first_seen_date = DATE_SUB(@submission_date, INTERVAL 27 day)
),
clients_first_seen_28_days_ago_with_days_seen AS (
  SELECT
    clients_first_seen_28_days_ago.*,
    cls.days_active_bits,
    cls.days_seen_bits,
    cls.days_visited_1_uri_bits,
    cls.days_interacted_bits,
  FROM
    clients_first_seen_28_days_ago
  LEFT JOIN
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen` cls
    ON clients_first_seen_28_days_ago.client_id = cls.client_id
    AND cls.submission_date = @submission_date
)
SELECT
  client_id,
  sample_id,
  first_seen_date,
  architecture,
  app_name,
  locale,
  app_version,
  xpcom_abi,
  distribution_id,
  partner_distribution_version,
  partner_distributor,
  partner_distributor_channel,
  partner_id,
  attribution_campaign,
  attribution_content,
  attribution_msclkid,
  attribution_dltoken,
  attribution_dlsource,
  attribution_experiment,
  attribution_medium,
  attribution_source,
  attribution_ua,
  attribution_variation,
  apple_model_id,
  isp_name,
  normalized_channel,
  country,
  normalized_os,
  normalized_os_version,
  startup_profile_selection_reason,
  installation_first_seen_admin_user,
  installation_first_seen_default_path,
  installation_first_seen_failure_reason,
  installation_first_seen_from_msi,
  installation_first_seen_install_existed,
  installation_first_seen_installer_type,
  installation_first_seen_other_inst,
  installation_first_seen_other_msix_inst,
  installation_first_seen_profdir_existed,
  installation_first_seen_silent,
  installation_first_seen_version,
  windows_build_number,
  windows_version,
  profile_group_id,
  COALESCE(
    days_seen_bits,
    mozfun.bits28.from_string('0000000000000000000000000000')
  ) AS days_seen_bits,
  COALESCE(
    days_active_bits,
    mozfun.bits28.from_string('0000000000000000000000000000')
  ) AS days_active_bits,
  COALESCE(
    days_visited_1_uri_bits,
    mozfun.bits28.from_string('0000000000000000000000000000')
  ) AS days_visited_1_uri_bits,
  COALESCE(
    days_interacted_bits,
    mozfun.bits28.from_string('0000000000000000000000000000')
  ) AS days_interacted_bits,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('1111111000000000000000000000') & days_seen_bits) >= 5,
    FALSE
  ) AS activated,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0111111111111111111111111111') & days_seen_bits) > 0,
    FALSE
  ) AS returned_second_day,
  COALESCE(
    BIT_COUNT(
      mozfun.bits28.from_string(
        '0111111111111111111111111111'
      ) & days_visited_1_uri_bits & days_interacted_bits
    ) > 0,
    FALSE
  ) AS qualified_second_day,
  COALESCE(
    BIT_COUNT(mozfun.bits28.from_string('0000000000000000000001111111') & days_seen_bits) > 0,
    FALSE
  ) AS retained_week4,
  COALESCE(
    BIT_COUNT(
      mozfun.bits28.from_string(
        '0000000000000000000001111111'
      ) & days_visited_1_uri_bits & days_interacted_bits
    ) > 0,
    FALSE
  ) AS qualified_week4,
  @submission_date AS submission_date,
  app_build_id
FROM
  clients_first_seen_28_days_ago_with_days_seen
