CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.glean_baseline_clients_first_seen`
AS
SELECT
  submission_date,
  first_seen_date,
  sample_id,
  client_id,
  attribution,
  `distribution`,
  attribution_ext,
  JSON_VALUE(attribution_ext.dlsource) AS attribution_dlsource,
  JSON_VALUE(attribution_ext.dltoken) AS attribution_dltoken,
  JSON_VALUE(attribution_ext.ua) AS attribution_ua,
  JSON_VALUE(attribution_ext.experiment) AS attribution_experiment,
  JSON_VALUE(attribution_ext.variation) AS attribution_variation,
  distribution_ext,
  legacy_telemetry_client_id,
  legacy_telemetry_profile_group_id,
  country,
  distribution_id,
  windows_build_number,
  locale,
  normalized_os,
  app_display_version,
  normalized_channel,
  normalized_os_version,
  isp,
  IF(
    LOWER(IFNULL(isp, '')) <> "browserstack"
    AND LOWER(IFNULL(COALESCE(distribution_id, distribution.name), '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop,
  mozfun.norm.glean_windows_version_info(
    normalized_os,
    normalized_os_version,
    windows_build_number
  ) AS windows_version
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1`
