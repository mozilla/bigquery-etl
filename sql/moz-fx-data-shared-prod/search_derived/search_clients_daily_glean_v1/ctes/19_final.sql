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
  -- days from the first run to the day this row reports into, derived from the
  -- profile_creation_date the row publishes so the two columns cannot disagree.
  -- submission_date is UTC against a client-local creation date, so the count can sit a day
  -- either side of the client's own.
  UNIX_DATE(submission_date) - profile_creation_date AS profile_age_in_days,
  serp_searches_organic_count,
  serp_searches_tagged_count,
  serp_follow_on_searches_tagged_count,
  sap_counts_total,
  serp_counts_total,
  serp_ad_click_target,
  serp_ad_clicks_sum,
  serp_ad_clicks_tagged_sum,
  serp_ad_clicks_organic_sum,
  serp_searches_with_ads_tagged_count,
  serp_searches_with_ads_organic_count,
  serp_ad_blocker_inferred,
  serp_non_ad_link_clicks_sum, -- NEW
  serp_other_engagements_sum, -- NEW
  serp_ads_loaded_sum, -- NEW
  serp_ads_visible_sum, -- NEW
  serp_ads_blocked_sum, -- NEW
  serp_ads_notshowing_sum, -- NEW
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
