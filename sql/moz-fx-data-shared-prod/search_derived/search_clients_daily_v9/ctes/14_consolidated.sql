SELECT
  serp_submission_date AS submission_date,
  serp_client_id AS client_id,
  serp_legacy_telemetry_client_id AS legacy_telemetry_client_id, -- NEW
  serp_provider_id AS engine, -- this is normalized in serp_events_with_client_info
  CASE
    WHEN serp_provider_id IS NULL
      AND sap_normalized_engine IS NOT NULL
      THEN sap_normalized_engine
    ELSE serp_provider_id
  END AS normalized_engine,
  serp_search_access_point AS source,
  serp_partner_code AS partner_code, -- NEW
  serp_country AS country,
  NULL AS addon_version,
  serp_app_version AS app_version,
  serp_windows_build_number AS windows_build_number, -- NEW
  serp_distribution_id AS distribution_id,
  serp_locale AS locale,
  serp_region_home_region AS user_pref_browser_search_region,
  NULL AS search_cohort,
  serp_os AS os,
  serp_normalized_os AS normalized_os, -- NEW
  serp_os_version AS os_version,
  serp_normalized_os_version AS normalized_os_version, -- NEW
  serp_channel AS channel,
  serp_normalized_channel AS normalized_channel, -- NEW
  serp_usage_is_default_browser AS is_default_browser,
  serp_profile_creation_date AS profile_creation_date,
  serp_default_search_engine_display_name AS default_search_engine,
  serp_default_search_engine_load_path AS default_search_engine_data_load_path,
  serp_default_search_engine_submission_url AS default_search_engine_data_submission_url,
  serp_default_search_engine_partner_code AS default_search_engine_partner_code, -- NEW
  serp_default_search_engine_provider_id AS default_search_engine_provider_id, -- NEW
  serp_default_search_engine_overridden AS default_search_engine_overridden, -- NEW
  serp_default_private_search_engine_display_name AS default_private_search_engine,
  serp_default_private_search_engine_load_path AS default_private_search_engine_data_load_path,
  serp_default_private_search_engine_submission_url AS default_private_search_engine_data_submission_url,
  serp_default_private_search_engine_partner_code AS default_private_search_engine_partner_code, -- NEW
  serp_default_private_search_engine_provider_id AS default_private_search_engine_provider_id, -- NEW
  serp_default_private_search_engine_overridden AS default_private_search_engine_overridden, -- NEW
  serp_sample_id AS sample_id,
  serp_subsession_start_time AS subsession_start_time, -- NEW
  serp_subsession_end_time AS subsession_end_time, -- NEW
  serp_subsession_counter AS subsession_counter, -- NEW
  NULL AS subsessions_hours_sum,
  serp_sessions_started_on_this_day AS sessions_started_on_this_day,
  serp_overridden_by_third_party AS overridden_by_third_party, -- NEW
  NULL AS active_addons_count_mean,
  serp_max_concurrent_tab_count_max AS max_concurrent_tab_count_max,
  serp_scalar_parent_browser_engagement_tab_open_event_count_sum AS tab_open_event_count_sum,
  serp_active_hours_sum AS active_hours_sum,
  serp_scalar_parent_browser_engagement_total_uri_count_sum AS total_uri_count,
  serp_experiments AS experiments,
  NULL AS scalar_parent_urlbar_searchmode_bookmarkmenu_sum,
  NULL AS scalar_parent_urlbar_searchmode_handoff_sum,
  NULL AS scalar_parent_urlbar_searchmode_keywordoffer_sum,
  NULL AS scalar_parent_urlbar_searchmode_oneoff_sum,
  NULL AS scalar_parent_urlbar_searchmode_other_sum,
  NULL AS scalar_parent_urlbar_searchmode_shortcut_sum,
  NULL AS scalar_parent_urlbar_searchmode_tabmenu_sum,
  NULL AS scalar_parent_urlbar_searchmode_tabtosearch_sum,
  NULL AS scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum,
  NULL AS scalar_parent_urlbar_searchmode_topsites_newtab_sum,
  NULL AS scalar_parent_urlbar_searchmode_topsites_urlbar_sum,
  NULL AS scalar_parent_urlbar_searchmode_touchbar_sum,
  NULL AS scalar_parent_urlbar_searchmode_typed_sum,
  serp_profile_age_in_days AS profile_age_in_days,
  serp_searches_organic_count AS organic,
  serp_searches_tagged_count AS tagged_sap,
  serp_searches_tagged_count AS tagged_serp,
  serp_follow_on_searches_tagged_count AS tagged_follow_on,
  sap,
  serp_counts,
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
  NULL AS unknown,
  NULL AS is_sap_monetizable, -- REVISIT
  serp_has_adblocker_addon AS has_adblocker_addon,
  serp_policies_is_enterprise AS policies_is_enterprise,
  serp_os_version_major AS os_version_major,
  serp_os_version_minor AS os_version_minor,
  serp_profile_group_id AS profile_group_id
FROM
  `search_derived.search_clients_daily_v9.join_sap_serp_cte`
WHERE
  serp_submission_date IS NOT NULL
