CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.glean_baseline_clients_first_seen`
AS
SELECT
  bcfs.submission_date,
  bcfs.first_seen_date,
  bcfs.sample_id,
  bcfs.client_id,
  bcfs.attribution,
  bcfs.`distribution`,
  bcfs.attribution_ext,
  JSON_VALUE(bcfs.attribution_ext.dlsource) AS attribution_dlsource,
  JSON_VALUE(bcfs.attribution_ext.msclkid) AS attribution_msclkid,
  JSON_VALUE(bcfs.attribution_ext.dltoken) AS attribution_dltoken,
  JSON_VALUE(bcfs.attribution_ext.ua) AS attribution_ua,
  JSON_VALUE(bcfs.attribution_ext.experiment) AS attribution_experiment,
  JSON_VALUE(bcfs.attribution_ext.variation) AS attribution_variation,
  JSON_VALUE(bcfs.distribution_ext.distributionVersion) AS distribution_version,
  JSON_VALUE(bcfs.distribution_ext.distributor) AS distributor,
  JSON_VALUE(bcfs.distribution_ext.partnerId) AS distribution_partner_id,
  JSON_VALUE(bcfs.distribution_ext.distributorChannel) AS distributor_channel,
  bcfs.distribution_ext,
  bcfs.legacy_telemetry_client_id,
  bcfs.legacy_telemetry_profile_group_id,
  bcfs.country,
  bcfs.distribution_id,
  bcfs.windows_build_number,
  bcfs.locale,
  bcfs.normalized_os,
  bcfs.app_display_version,
  bcfs.app_build_id,
  bcfs.normalized_channel,
  bcfs.normalized_os_version,
  bcfs.isp,
  bcfs.startup_profile_selection_reason_first,
  bcfs.architecture,
  IF(
    LOWER(IFNULL(bcfs.isp, '')) <> "browserstack"
    AND LOWER(
      IFNULL(COALESCE(bcfs.distribution_id, bcfs.distribution.name), '')
    ) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop,
  mozfun.norm.glean_windows_version_info(
    bcfs.normalized_os,
    bcfs.normalized_os_version,
    bcfs.windows_build_number
  ) AS windows_version,
  CASE
    WHEN LOWER(IFNULL(bcfs.isp, '')) = 'browserstack'
      THEN CONCAT('Firefox Desktop', ' ', isp)
    WHEN LOWER(
        IFNULL(COALESCE(bcfs.distribution_id, distribution_mapping.distribution_id), '')
      ) = 'mozillaonline'
      THEN CONCAT(
          'Firefox Desktop',
          ' ',
          COALESCE(bcfs.distribution_id, distribution_mapping.distribution_id)
        )
    ELSE 'Firefox Desktop'
  END AS normalized_app_name
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` bcfs
LEFT JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.desktop_dau_distribution_id_history_v1` AS distribution_mapping
  USING (submission_date, client_id)
