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
    --
    stub_attr_logs.first_seen_date AS stub_attr_logs_first_seen_date,
    stub_attr_logs.ga_client_id,
    stub_attr_logs.stub_session_id,
    --stub_attr_logs.dl_token
    --
FROM `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` cfs
LEFT JOIN 
(
    --note: dl_token_ga_attribution_lookup_v1 currently does only have 1 row per dl_token, 
    --but just to be safe, forcing it to pick the first reported one if there are ever 2 or more
    SELECT 
    first_seen_date,
    ga_client_id,
    stub_session_id,
    dl_token
    FROM `moz-fx-data-shared-prod.stub_attribution_service_derived.dl_token_ga_attribution_lookup_v1`
    WHERE coalesce(ga_client_id, '') <> ''
    and coalesce(stub_session_id, '') <> ''
    and coalesce(dl_token, '') <> ''
    QUALIFY row_number() over (partition by dl_token order by first_seen_date ASC) = 1
) stub_attr_logs
ON JSON_VALUE(cfs.attribution_ext.dltoken) = stub_attr_logs.dl_token
LEFT JOIN 
(
   SELECT flag AS website,
    --ga_client_id
    --ga_session_id
    session_date
    --session_start_timestamp
    --is_first_session
    --session_number
    --time_on_site
    --pageviews
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
    last_reported_stub_session_id,
    all_reported_stub_session_ids,
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
    ad_crosschannel_default_channel_group
FROM `moz-fx-data-shared-prod.telemetry.ga4_sessions_firefoxcom_mozillaorg_combined`
 ) ga4
ON ?.? = ?.?
WHERE ??