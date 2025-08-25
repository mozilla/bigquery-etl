CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.cfs_ga4_attr`
AS
SELECT
  cfs.submission_date,
  cfs.first_seen_date,
  cfs.sample_id,
  cfs.client_id,
  cfs.attribution,
  cfs.distribution,
  cfs.attribution_ext,
  cfs.distribution_ext,
  cfs.legacy_telemetry_client_id,
  cfs.legacy_telemetry_profile_group_id,
  cfs.country,
  cfs.distribution_id,
  cfs.windows_build_number,
  cfs.locale,
  cfs.normalized_os,
  cfs.app_display_version,
  cfs.normalized_channel,
  cfs.normalized_os_version,
  cfs.isp,
  JSON_VALUE(cfs.attribution_ext.dlsource) AS attribution_dlsource,
  JSON_VALUE(cfs.attribution_ext.dltoken) AS attribution_dltoken,
  JSON_VALUE(cfs.attribution_ext.ua) AS attribution_ua,
  JSON_VALUE(cfs.attribution_ext.experiment) AS attribution_experiment,
  JSON_VALUE(cfs.attribution_ext.variation) AS attribution_variation,
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
  ) AS windows_version,
  stub_attr_logs.first_seen_date AS stub_attr_logs_first_seen_date,
  stub_attr_logs.ga_client_id AS stub_attr_logs_ga_client_id,
  stub_attr_logs.stub_session_id AS stub_attr_logs_stub_session_id,
  stub_attr_logs.dl_token AS stub_attr_logs_dl_token,
  stub_attr_logs.download_source AS stub_attr_logs_download_source,
  ga4.flag AS ga4_website,
  ga4.ga_client_id AS ga4_ga_client_id,
  ga4.session_date AS ga4_session_date,
  ga4.session_start_timestamp AS ga4_session_start_timestamp,
  ga4.country AS ga4_country,
  ga4.region AS ga4_region,
  ga4.city AS ga4_city,
  ga4.device_category AS ga4_device_category,
  ga4.mobile_device_model AS ga4_mobile_device_model,
  ga4.mobile_device_string AS ga4_mobile_device_string,
  ga4.os AS ga4_operating_system,
  ga4.os_version AS ga4_operating_system_version,
  ga4.`language` AS ga4_language,
  ga4.browser AS ga4_browser,
  ga4.browser_version AS ga4_browser_version,
  ga4.first_campaign_from_event_params AS ga4_first_campaign_from_event_params,
  ga4.distinct_campaigns_from_event_params AS ga4_distinct_campaigns_from_event_params,
  ga4.first_source_from_event_params AS ga4_first_source_from_event_params,
  ga4.distinct_sources_from_event_params AS ga4_distinct_sources_from_event_params,
  ga4.first_medium_from_event_params AS ga4_first_medium_from_event_params,
  ga4.distinct_mediums_from_event_params AS ga4_distinct_mediums_from_event_params,
  ga4.first_content_from_event_params AS ga4_first_content_from_event_params,
  ga4.distinct_contents_from_event_params AS ga4_distinct_contents_from_event_params,
  ga4.first_term_from_event_params AS ga4_first_term_from_event_params,
  ga4.distinct_terms_from_event_params AS ga4_distinct_terms_from_event_params,
  ga4.manual_campaign_id AS ga4_manual_campaign_id,
  ga4.manual_campaign_name AS ga4_manual_campaign_name,
  ga4.manual_source AS ga4_manual_source,
  ga4.manual_medium AS ga4_manual_medium,
  ga4.manual_term AS ga4_manual_term,
  ga4.manual_content AS ga4_manual_content,
  ga4.gclid AS ga4_gclid,
  ga4.gclid_array AS ga4_gclid_array,
  ga4.had_download_event AS ga4_had_download_event,
  ga4.firefox_desktop_downloads AS ga4_firefox_desktop_downloads,
  ga4.last_reported_install_target AS ga4_last_reported_install_target,
  ga4.all_reported_install_targets AS ga4_all_reported_install_targets,
  ga4.stub_session_id AS ga4_stub_session_id,
  ga4.landing_screen AS ga4_landing_screen,
  ga4.ad_google_campaign AS ga4_ad_google_campaign,
  ga4.ad_google_campaign_id AS ga4_ad_google_campaign_id,
  ga4.ad_google_group AS ga4_ad_google_group,
  ga4.ad_google_group_id AS ga4_ad_google_group_id,
  ga4.ad_google_account AS ga4_ad_google_account,
  ga4.ad_crosschannel_source AS ga4_ad_crosschannel_source,
  ga4.ad_crosschannel_medium AS ga4_ad_crosschannel_medium,
  ga4.ad_crosschannel_campaign AS ga4_ad_crosschannel_campaign,
  ga4.ad_crosschannel_campaign_id AS ga4_ad_crosschannel_campaign_id,
  ga4.ad_crosschannel_source_platform AS ga4_ad_crosschannel_source_platform,
  ga4.ad_crosschannel_primary_channel_group AS ga4_ad_crosschannel_primary_channel_group,
  ga4.ad_crosschannel_default_channel_group AS ga4_ad_crosschannel_default_channel_group
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` cfs
LEFT JOIN
  (
    SELECT
      dl_token,
      ga_client_id,
      stub_session_id,
      download_source,
      first_seen_date
    FROM
      `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v2`
    WHERE
      COALESCE(ga_client_id, '') <> ''
      AND COALESCE(stub_session_id, '') <> ''
      AND COALESCE(dl_token, '') <> ''
    QUALIFY
      ROW_NUMBER() OVER (PARTITION BY dl_token ORDER BY first_seen_date ASC) = 1
  ) stub_attr_logs
  ON JSON_VALUE(cfs.attribution_ext.dltoken) = stub_attr_logs.dl_token
LEFT JOIN
  (
    SELECT
      flag,
      ga_client_id,
      session_date,
      session_start_timestamp,
      country,
      region,
      city,
      device_category,
      mobile_device_model,
      mobile_device_string,
      os,
      os_version,
      `language`,
      browser,
      browser_version,
      first_campaign_from_event_params,
      distinct_campaigns_from_event_params,
      first_source_from_event_params,
      distinct_sources_from_event_params,
      first_medium_from_event_params,
      distinct_mediums_from_event_params,
      first_content_from_event_params,
      distinct_contents_from_event_params,
      first_term_from_event_params,
      distinct_terms_from_event_params,
      manual_campaign_id,
      manual_campaign_name,
      manual_source,
      manual_medium,
      manual_term,
      manual_content,
      gclid,
      gclid_array,
      had_download_event,
      firefox_desktop_downloads,
      last_reported_install_target,
      all_reported_install_targets,
      landing_screen,
      ad_google_campaign,
      ad_google_campaign_id,
      ad_google_group,
      ad_google_group_id,
      ad_google_account,
      ad_crosschannel_source,
      ad_crosschannel_medium,
      ad_crosschannel_campaign,
      ad_crosschannel_campaign_id,
      ad_crosschannel_source_platform,
      ad_crosschannel_primary_channel_group,
      ad_crosschannel_default_channel_group,
      stub_session_id,
      IF(stub_session_id IS NOT NULL, 1, 0) AS has_stub_session_id
    FROM
      `moz-fx-data-shared-prod.telemetry.ga4_sessions_firefoxcom_mozillaorg_combined`
    LEFT JOIN
      UNNEST(all_reported_stub_session_ids) stub_session_id
  ) ga4
  ON stub_attr_logs.ga_client_id = ga4.ga_client_id
  AND stub_attr_logs.stub_session_id = ga4.stub_session_id
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      cfs.client_id
    ORDER BY
      COALESCE(has_stub_session_id, 0) DESC,
      ga4.stub_session_id ASC
  ) = 1
