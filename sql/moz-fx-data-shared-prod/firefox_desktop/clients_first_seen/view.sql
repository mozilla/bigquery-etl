CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.clients_first_seen`
AS
SELECT
  COALESCE(b.client_id, m.client_id) AS client_id,
  COALESCE(b.sample_id, m.sample_id) AS sample_id,
  m.first_seen_date AS metric_first_seen_date,
  m.submission_date AS metric_submission_date,
  m.normalized_channel AS metric_normalized_channel,
  m.profile_group_id AS metric_profile_group_id,
  m.apple_model_id AS metric_apple_model_id,
  m.default_search_engine AS metric_default_search_engine,
  m.xpcom_abi AS metric_xpcom_abi,
  m.installation_first_seen_admin_user AS metric_installation_first_seen_admin_user,
  m.installation_first_seen_default_path AS metric_installation_first_seen_default_path,
  m.installation_first_seen_failure_reason AS metric_installation_first_seen_failure_reason,
  m.installation_first_seen_from_msi AS metric_installation_first_seen_from_msi,
  m.installation_first_seen_install_existed AS metric_installation_first_seen_install_existed,
  m.installation_first_seen_installer_type AS metric_installation_first_seen_installer_type,
  m.installation_first_seen_other_inst AS metric_installation_first_seen_other_inst,
  m.installation_first_seen_other_msix_inst AS metric_installation_first_seen_other_msix_inst,
  m.installation_first_seen_profdir_existed AS metric_installation_first_seen_profdir_existed,
  m.installation_first_seen_silent AS metric_installation_first_seen_silent,
  m.installation_first_seen_version AS metric_installation_first_seen_version,
  b.submission_date AS baseline_submission_date,
  b.first_seen_date AS baseline_first_seen_date,
  b.attribution AS baseline_attribution,
  b.distribution AS baseline_distribution,
  b.attribution_ext AS baseline_attribution_ext,
  b.attribution_dlsource AS baseline_attribution_dlsource,
  b.attribution_msclkid AS baseline_attribution_msclkid,
  b.attribution_dltoken AS baseline_attribution_dltoken,
  b.attribution_ua AS baseline_attribution_ua,
  b.attribution_experiment AS baseline_attribution_experiment,
  b.attribution_variation AS baseline_attribution_variation,
  b.distribution_version AS baseline_distribution_version,
  b.distributor AS baseline_distributor,
  b.distribution_partner_id AS baseline_distribution_partner_id,
  b.distributor_channel AS baseline_distributor_channel,
  b.distribution_ext AS baseline_distribution_ext,
  b.legacy_telemetry_client_id AS baseline_legacy_telemetry_client_id,
  b.legacy_telemetry_profile_group_id AS baseline_legacy_telemetry_profile_group_id,
  b.country AS baseline_country,
  b.distribution_id AS baseline_distribution_id,
  b.windows_build_number AS baseline_windows_build_number,
  b.locale AS baseline_locale,
  b.normalized_os AS baseline_normalized_os,
  b.app_display_version AS baseline_app_display_version,
  b.normalized_channel AS baseline_normalized_channel,
  b.normalized_os_version AS baseline_normalized_os_version,
  b.isp AS baseline_isp,
  b.startup_profile_selection_reason_first AS baseline_startup_profile_selection_reason_first,
  b.architecture AS baseline_architecture,
  b.is_desktop AS baseline_is_desktop,
  b.windows_version AS baseline_windows_version,
  b.normalized_app_name AS baseline_normalized_app_name,
  b.app_build_id AS baseline_app_build_id
FROM
  `moz-fx-data-shared-prod.firefox_desktop.metrics_clients_first_seen` m
FULL OUTER JOIN
  (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.firefox_desktop.glean_baseline_clients_first_seen`
    WHERE
      submission_date <= current_date --required since this view has a requirement to filter on submission date
  ) b
  ON m.client_id = b.client_id
