-- join_sap_serp_cte
SELECT
  sap_final_cte.client_id AS sap_client_id,
  serp_final_cte.client_id AS serp_client_id,
  sap_final_cte.submission_date AS sap_submission_date,
  serp_final_cte.submission_date AS serp_submission_date,
  sap_final_cte.normalized_engine AS sap_normalized_engine,
  serp_final_cte.serp_provider_id,
  sap_final_cte.partner_code AS sap_partner_code,
  serp_final_cte.partner_code AS serp_partner_code,
  sap_final_cte.source AS sap_search_access_point,
  serp_final_cte.serp_search_access_point,
  sap_final_cte.sample_id AS sap_sample_id,
  serp_final_cte.sample_id AS serp_sample_id,
  sap_final_cte.profile_group_id AS sap_profile_group_id,
  sap_final_cte.legacy_telemetry_client_id AS sap_legacy_telemetry_client_id,
  serp_final_cte.legacy_telemetry_client_id AS serp_legacy_telemetry_client_id,
  sap_final_cte.country AS sap_country,
  sap_final_cte.app_version AS sap_app_version,
  sap_final_cte.channel AS sap_channel,
  sap_final_cte.normalized_channel AS sap_normalized_channel,
  sap_final_cte.locale AS sap_locale,
  sap_final_cte.os AS sap_os,
  sap_final_cte.normalized_os AS sap_normalized_os,
  sap_final_cte.os_version AS sap_os_version,
  sap_final_cte.normalized_os_version AS sap_normalized_os_version,
  sap_final_cte.windows_build_number AS sap_windows_build_number,
  sap_final_cte.distribution_id AS sap_distribution_id,
  sap_final_cte.profile_creation_date AS sap_profile_creation_date,
  sap_final_cte.region_home_region AS sap_region_home_region,
  sap_final_cte.usage_is_default_browser AS sap_usage_is_default_browser,
  sap_final_cte.search_engine_default_display_name AS sap_default_search_engine_display_name,
  sap_final_cte.search_engine_default_load_path AS sap_default_search_engine_load_path,
  sap_final_cte.search_engine_default_partner_code AS sap_default_search_engine_partner_code,
  sap_final_cte.search_engine_default_provider_id AS sap_default_search_engine_provider_id,
  sap_final_cte.search_engine_default_submission_url AS sap_default_search_engine_submission_url,
  sap_final_cte.search_engine_default_overridden_by_third_party AS sap_default_search_engine_overridden,
  sap_final_cte.search_engine_private_display_name AS sap_default_private_search_engine_display_name,
  sap_final_cte.search_engine_private_load_path AS sap_default_private_search_engine_load_path,
  sap_final_cte.search_engine_private_partner_code AS sap_default_private_search_engine_partner_code,
  sap_final_cte.search_engine_private_provider_id AS sap_default_private_search_engine_provider_id,
  sap_final_cte.search_engine_private_submission_url AS sap_default_private_search_engine_submission_url,
  sap_final_cte.search_engine_private_overridden_by_third_party AS sap_default_private_search_engine_overridden,
  sap_final_cte.provider_name AS sap_provider_name,
  sap_final_cte.provider_id AS sap_provider_id,
  sap_final_cte.overridden_by_third_party AS sap_overridden_by_third_party,
  sap_final_cte.subsession_start_time AS sap_subsession_start_time,
  sap_final_cte.subsession_end_time AS sap_subsession_end_time,
  sap_final_cte.subsession_counter AS sap_subsession_counter,
  sap_final_cte.experiments AS sap_experiments,
  sap_final_cte.has_adblocker_addon AS sap_has_adblocker_addon,
  sap_final_cte.policies_is_enterprise AS sap_policies_is_enterprise,
  sap_final_cte.profile_age_in_days AS sap_profile_age_in_days,
  sap_final_cte.sap,
  sap_final_cte.sessions_started_on_this_day AS sap_sessions_started_on_this_day,
  sap_final_cte.active_hours_sum AS sap_active_hours_sum,
  sap_final_cte.scalar_parent_browser_engagement_tab_open_event_count_sum AS sap_scalar_parent_browser_engagement_tab_open_event_count_sum,
  sap_final_cte.scalar_parent_browser_engagement_total_uri_count_sum AS sap_scalar_parent_browser_engagement_total_uri_count_sum,
  sap_final_cte.concurrent_tab_count_max AS sap_concurrent_tab_count_max,
  serp_final_cte.profile_group_id AS serp_profile_group_id,
  serp_final_cte.country AS serp_country,
  serp_final_cte.app_version AS serp_app_version,
  serp_final_cte.channel AS serp_channel,
  serp_final_cte.normalized_channel AS serp_normalized_channel,
  serp_final_cte.locale AS serp_locale,
  serp_final_cte.os AS serp_os,
  serp_final_cte.normalized_os AS serp_normalized_os,
  serp_final_cte.os_version AS serp_os_version,
  serp_final_cte.normalized_os_version AS serp_normalized_os_version,
  serp_final_cte.os_version_major AS serp_os_version_major,
  serp_final_cte.os_version_minor AS serp_os_version_minor,
  serp_final_cte.windows_build_number AS serp_windows_build_number,
  serp_final_cte.distribution_id AS serp_distribution_id,
  serp_final_cte.profile_creation_date AS serp_profile_creation_date,
  serp_final_cte.region_home_region AS serp_region_home_region,
  serp_final_cte.usage_is_default_browser AS serp_usage_is_default_browser,
  serp_final_cte.search_engine_default_display_name AS serp_default_search_engine_display_name,
  serp_final_cte.search_engine_default_load_path AS serp_default_search_engine_load_path,
  serp_final_cte.search_engine_default_partner_code AS serp_default_search_engine_partner_code,
  serp_final_cte.search_engine_default_provider_id AS serp_default_search_engine_provider_id,
  serp_final_cte.search_engine_default_submission_url AS serp_default_search_engine_submission_url,
  serp_final_cte.search_engine_default_overridden_by_third_party AS serp_default_search_engine_overridden,
  serp_final_cte.search_engine_private_display_name AS serp_default_private_search_engine_display_name,
  serp_final_cte.search_engine_private_load_path AS serp_default_private_search_engine_load_path,
  serp_final_cte.search_engine_private_partner_code AS serp_default_private_search_engine_partner_code,
  serp_final_cte.search_engine_private_provider_id AS serp_default_private_search_engine_provider_id,
  serp_final_cte.search_engine_private_submission_url AS serp_default_private_search_engine_submission_url,
  serp_final_cte.search_engine_private_overridden_by_third_party AS serp_default_private_search_engine_overridden,
  serp_final_cte.overridden_by_third_party AS serp_overridden_by_third_party,
  serp_final_cte.subsession_start_time AS serp_subsession_start_time,
  serp_final_cte.subsession_end_time AS serp_subsession_end_time,
  serp_final_cte.subsession_counter AS serp_subsession_counter,
  serp_final_cte.experiments AS serp_experiments,
  serp_final_cte.has_adblocker_addon AS serp_has_adblocker_addon,
  serp_final_cte.policies_is_enterprise AS serp_policies_is_enterprise,
  serp_final_cte.ad_click_target AS serp_ad_click_target,
  serp_final_cte.serp_ad_blocker_inferred AS serp_ad_blocker_inferred,
  serp_final_cte.serp_follow_on_searches_tagged_count AS serp_follow_on_searches_tagged_count,
  serp_final_cte.serp_searches_tagged_count AS serp_searches_tagged_count,
  serp_final_cte.serp_searches_organic_count AS serp_searches_organic_count,
  serp_final_cte.serp_with_ads_organic_count AS serp_with_ads_organic_count,
  serp_final_cte.serp_with_ads_tagged_count AS serp_with_ads_tagged_count,
  serp_final_cte.serp_ad_clicks_tagged_count AS serp_ad_clicks_tagged_count,
  serp_final_cte.serp_ad_clicks_organic_count AS serp_ad_clicks_organic_count,
  serp_final_cte.num_ad_clicks AS serp_num_ad_clicks,
  serp_final_cte.num_non_ad_link_clicks AS serp_num_non_ad_link_clicks,
  serp_final_cte.num_other_engagements AS serp_num_other_engagements,
  serp_final_cte.num_ads_loaded AS serp_num_ads_loaded,
  serp_final_cte.num_ads_visible AS serp_num_ads_visible,
  serp_final_cte.num_ads_blocked AS serp_num_ads_blocked,
  serp_final_cte.num_ads_notshowing AS serp_num_ads_notshowing,
  serp_final_cte.profile_age_in_days AS serp_profile_age_in_days,
  serp_final_cte.serp_counts,
  serp_final_cte.sessions_started_on_this_day AS serp_sessions_started_on_this_day,
  serp_final_cte.active_hours_sum AS serp_active_hours_sum,
  serp_final_cte.serp_scalar_parent_browser_engagement_tab_open_event_count_sum AS serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
  serp_final_cte.serp_scalar_parent_browser_engagement_total_uri_count_sum AS serp_scalar_parent_browser_engagement_total_uri_count_sum,
  serp_final_cte.max_concurrent_tab_count_max AS serp_max_concurrent_tab_count_max
FROM
  sap_final_cte
FULL OUTER JOIN
  serp_final_cte
  ON (
    sap_final_cte.client_id = serp_final_cte.client_id
    OR (sap_final_cte.client_id IS NULL AND serp_final_cte.client_id IS NULL)
  )
  AND (
    sap_final_cte.submission_date = serp_final_cte.submission_date
    OR (sap_final_cte.submission_date IS NULL AND serp_final_cte.submission_date IS NULL)
  )
  AND (
    sap_final_cte.normalized_engine = serp_final_cte.serp_provider_id
    OR (sap_final_cte.normalized_engine IS NULL AND serp_final_cte.serp_provider_id IS NULL)
  )
  AND (
    sap_final_cte.source = serp_final_cte.serp_search_access_point
    OR (sap_final_cte.source IS NULL AND serp_final_cte.serp_search_access_point IS NULL)
  )
-- and sap_final_cte.search_access_point = serp_final_cte.serp_search_access_point -- rename)
WHERE
  sap_final_cte.normalized_channel IN ('nightly', 'beta')
  OR serp_final_cte.normalized_channel IN ('nightly', 'beta')
  AND client_id IN (
    '28ea5671-012b-4207-af58-0af5af2fa1ae',
    '7c3b8c4f-900b-424a-96f8-b3d0bbf7757c',
    'f811c097-b567-419e-bc21-2f50783eb468',
    'a90a56a1-0a82-41d0-8d9b-9cd906196a2f'
  )
ORDER BY
  client_id
