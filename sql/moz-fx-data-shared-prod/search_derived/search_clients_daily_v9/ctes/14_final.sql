SELECT
  submission_date,
  client_id,
  legacy_telemetry_client_id, -- NEW
  provider_id AS normalized_engine,
  search_access_point AS source,
  partner_code, -- NEW
  country,
  normalized_app_name,
  app_version,
  app_major_version,
  app_minor_version,
  app_patch_revision,
  windows_build_number, -- NEW
  distribution_id,
  locale,
  region_home_region AS user_pref_browser_search_region,
  os,
  normalized_os, -- NEW
  os_version,
  normalized_os_version, -- NEW
  channel,
  normalized_channel, -- NEW
  usage_is_default_browser AS is_default_browser,
  profile_creation_date,
  default_search_engine_display_name AS default_search_engine,
  default_search_engine_load_path AS default_search_engine_data_load_path,
  default_search_engine_submission_url AS default_search_engine_data_submission_url,
  default_search_engine_partner_code, -- NEW
  default_search_engine_provider_id, -- NEW
  default_search_engine_overridden, -- NEW
  default_private_search_engine_display_name AS default_private_search_engine,
  default_private_search_engine_load_path AS default_private_search_engine_data_load_path,
  default_private_search_engine_submission_url AS default_private_search_engine_data_submission_url,
  default_private_search_engine_partner_code, -- NEW
  default_private_search_engine_provider_id, -- NEW
  default_private_search_engine_overridden, -- NEW
  sample_id,
  ping_start_time, -- NEW
  ping_end_time, -- NEW
  ping_seq, -- NEW
  overridden_by_third_party, -- NEW
  max_concurrent_tab_count_max,
  experiments,
  profile_age_in_days,
  serp_searches_organic_count AS organic,
  serp_searches_tagged_count AS tagged_serp,
  serp_follow_on_searches_tagged_count AS tagged_follow_on,
  sap_counts_total,
  serp_counts_total,
  serp_ad_click_target AS ad_click_target,
  serp_num_ad_clicks AS ad_click,
  serp_ad_clicks_organic_count AS ad_click_organic,
  serp_with_ads_tagged_count AS search_with_ads,
  serp_with_ads_organic_count AS search_with_ads_organic,
  serp_ad_clicks_tagged_count AS ad_clicks_tagged,
  serp_ad_blocker_inferred AS ad_blocker_inferred,
  serp_num_non_ad_link_clicks AS num_non_ad_link_clicks, -- NEW
  serp_num_other_engagements AS num_other_engagements, -- NEW
  serp_num_ads_loaded AS num_ads_loaded, -- NEW
  serp_num_ads_visible AS num_ads_visible, -- NEW
  serp_num_ads_blocked AS num_ads_blocked, -- NEW
  serp_num_ads_notshowing AS num_ads_notshowing, -- NEW
  has_adblocker_addon,
  policies_is_enterprise,
  serp_os_version_major AS os_version_major,
  serp_os_version_minor AS os_version_minor,
  profile_group_id
FROM
  `search_derived.search_clients_daily_v9.join_sap_serp_cte`
