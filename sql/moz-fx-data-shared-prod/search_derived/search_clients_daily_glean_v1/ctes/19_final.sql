SELECT
  submission_date,
  client_id,
  legacy_telemetry_client_id, -- NEW
  provider_id AS normalized_engine,
  search_access_point AS source,
  partner_code, -- NEW
  country,
  -- neither serp_events nor the metrics ping populates normalized_app_name, and the SAP side's
  -- normalization returns one value for every row, so the literal is the whole of what this
  -- column can say. Set here rather than per side: a coalesce over three arms, one of them live,
  -- left it NULL wherever the SAP side was absent.
  'Firefox' AS normalized_app_name,
  app_version,
  app_major_version,
  app_minor_version,
  app_patch_revision,
  windows_build_number, -- NEW
  distribution_id,
  locale,
  region_home_region AS home_region,
  os,
  normalized_os, -- NEW
  os_version,
  normalized_os_version, -- NEW
  channel,
  normalized_channel, -- NEW
  usage_is_default_browser AS is_default_browser,
  profile_creation_date,
  default_search_engine_display_name,
  default_search_engine_load_path AS default_search_engine_data_load_path,
  default_search_engine_submission_url AS default_search_engine_data_submission_url,
  default_search_engine_partner_code, -- NEW
  default_search_engine_provider_id, -- NEW
  default_search_engine_overridden, -- NEW
  default_private_search_engine_display_name,
  default_private_search_engine_load_path AS default_private_search_engine_data_load_path,
  default_private_search_engine_submission_url AS default_private_search_engine_data_submission_url,
  default_private_search_engine_partner_code, -- NEW
  default_private_search_engine_provider_id, -- NEW
  default_private_search_engine_overridden, -- NEW
  sample_id,
  ping_start_time, -- NEW
  ping_end_time, -- NEW
  ping_seq, -- NEW
  max_concurrent_tab_count_max,
  experiments,
  profile_age_in_days,
  serp_searches_organic_count,
  serp_searches_tagged_count AS tagged_serp,
  serp_follow_on_searches_tagged_count AS tagged_follow_on,
  sap_counts_total,
  serp_counts_total,
  serp_ad_click_target,
  serp_num_ad_clicks AS ad_click_total,
  serp_ad_clicks_tagged_count AS ad_click_tagged,
  serp_ad_clicks_organic_count AS ad_click_organic,
  serp_searches_with_ads_tagged_count,
  serp_searches_with_ads_organic_count,
  serp_ad_blocker_inferred,
  serp_num_non_ad_link_clicks, -- NEW
  serp_num_other_engagements, -- NEW
  serp_num_ads_loaded, -- NEW
  serp_num_ads_visible, -- NEW
  serp_num_ads_blocked, -- NEW
  serp_num_ads_notshowing, -- NEW
  has_adblocker_addon,
  policies_is_enterprise,
  -- keep these after the coalesce, so they read the same os, os_version and
  -- windows_build_number the row publishes. NULL where that os_version does not parse.
  -- major and minor are deliberately identical on Windows: both are the release name.
  CASE
    WHEN mozfun.norm.os(os) = "Windows"
      THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
    ELSE CAST(mozfun.norm.truncate_version(os_version, "major") AS STRING)
  END AS os_version_major,
  CASE
    WHEN mozfun.norm.os(os) = "Windows"
      THEN mozfun.norm.windows_version_info(os, os_version, windows_build_number)
    ELSE CAST(mozfun.norm.truncate_version(os_version, "minor") AS STRING)
  END AS os_version_minor,
  profile_group_id,
  sap_provider_id, -- NEW
  sap_provider_name, -- NEW
  sap_overridden_by_third_party, -- NEW
  legacy_tagged_sap, -- NEW
  legacy_tagged_follow_on, -- NEW
  legacy_organic, -- NEW
  legacy_search_with_ads_tagged, -- NEW
  legacy_search_with_ads_organic, -- NEW
  legacy_ad_click_tagged, -- NEW
  legacy_ad_click_organic -- NEW
FROM
  `search_derived.search_clients_daily_glean_v1.join_sources_cte`
