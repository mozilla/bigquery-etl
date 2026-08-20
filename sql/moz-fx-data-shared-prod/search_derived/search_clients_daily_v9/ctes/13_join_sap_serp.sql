-- join_sap_serp_cte
SELECT
  -- columns present on both sides are COALESCEd with serp taking precedence
  COALESCE(serp_final_cte.client_id, sap_final_cte.client_id) AS serp_client_id,
  COALESCE(serp_final_cte.submission_date, sap_final_cte.submission_date) AS serp_submission_date,
  sap_final_cte.normalized_engine AS sap_normalized_engine,
  COALESCE(serp_final_cte.serp_provider_id, sap_final_cte.normalized_engine) AS serp_provider_id,
  COALESCE(serp_final_cte.partner_code, sap_final_cte.partner_code) AS serp_partner_code,
  COALESCE(
    serp_final_cte.serp_search_access_point,
    sap_final_cte.source
  ) AS serp_search_access_point,
  COALESCE(serp_final_cte.sample_id, sap_final_cte.sample_id) AS serp_sample_id,
  COALESCE(
    serp_final_cte.legacy_telemetry_client_id,
    sap_final_cte.legacy_telemetry_client_id
  ) AS serp_legacy_telemetry_client_id,
  sap_final_cte.sap,
  COALESCE(
    serp_final_cte.profile_group_id,
    sap_final_cte.profile_group_id
  ) AS serp_profile_group_id,
  COALESCE(serp_final_cte.country, sap_final_cte.country) AS serp_country,
  COALESCE(serp_final_cte.app_version, sap_final_cte.app_version) AS serp_app_version,
  COALESCE(serp_final_cte.channel, sap_final_cte.channel) AS serp_channel,
  COALESCE(
    serp_final_cte.normalized_channel,
    sap_final_cte.normalized_channel
  ) AS serp_normalized_channel,
  COALESCE(serp_final_cte.locale, sap_final_cte.locale) AS serp_locale,
  COALESCE(serp_final_cte.os, sap_final_cte.os) AS serp_os,
  COALESCE(serp_final_cte.normalized_os, sap_final_cte.normalized_os) AS serp_normalized_os,
  COALESCE(serp_final_cte.os_version, sap_final_cte.os_version) AS serp_os_version,
  COALESCE(
    serp_final_cte.normalized_os_version,
    sap_final_cte.normalized_os_version
  ) AS serp_normalized_os_version,
  serp_final_cte.os_version_major AS serp_os_version_major,
  serp_final_cte.os_version_minor AS serp_os_version_minor,
  COALESCE(
    serp_final_cte.windows_build_number,
    sap_final_cte.windows_build_number
  ) AS serp_windows_build_number,
  COALESCE(serp_final_cte.distribution_id, sap_final_cte.distribution_id) AS serp_distribution_id,
  COALESCE(
    serp_final_cte.profile_creation_date,
    sap_final_cte.profile_creation_date
  ) AS serp_profile_creation_date,
  COALESCE(
    serp_final_cte.region_home_region,
    sap_final_cte.region_home_region
  ) AS serp_region_home_region,
  COALESCE(
    serp_final_cte.usage_is_default_browser,
    sap_final_cte.usage_is_default_browser
  ) AS serp_usage_is_default_browser,
  COALESCE(
    serp_final_cte.search_engine_default_display_name,
    sap_final_cte.search_engine_default_display_name
  ) AS serp_default_search_engine_display_name,
  COALESCE(
    serp_final_cte.search_engine_default_load_path,
    sap_final_cte.search_engine_default_load_path
  ) AS serp_default_search_engine_load_path,
  COALESCE(
    serp_final_cte.search_engine_default_partner_code,
    sap_final_cte.search_engine_default_partner_code
  ) AS serp_default_search_engine_partner_code,
  COALESCE(
    serp_final_cte.search_engine_default_provider_id,
    sap_final_cte.search_engine_default_provider_id
  ) AS serp_default_search_engine_provider_id,
  COALESCE(
    serp_final_cte.search_engine_default_submission_url,
    sap_final_cte.search_engine_default_submission_url
  ) AS serp_default_search_engine_submission_url,
  COALESCE(
    serp_final_cte.search_engine_default_overridden_by_third_party,
    sap_final_cte.search_engine_default_overridden_by_third_party
  ) AS serp_default_search_engine_overridden,
  COALESCE(
    serp_final_cte.search_engine_private_display_name,
    sap_final_cte.search_engine_private_display_name
  ) AS serp_default_private_search_engine_display_name,
  COALESCE(
    serp_final_cte.search_engine_private_load_path,
    sap_final_cte.search_engine_private_load_path
  ) AS serp_default_private_search_engine_load_path,
  COALESCE(
    serp_final_cte.search_engine_private_partner_code,
    sap_final_cte.search_engine_private_partner_code
  ) AS serp_default_private_search_engine_partner_code,
  COALESCE(
    serp_final_cte.search_engine_private_provider_id,
    sap_final_cte.search_engine_private_provider_id
  ) AS serp_default_private_search_engine_provider_id,
  COALESCE(
    serp_final_cte.search_engine_private_submission_url,
    sap_final_cte.search_engine_private_submission_url
  ) AS serp_default_private_search_engine_submission_url,
  COALESCE(
    serp_final_cte.search_engine_private_overridden_by_third_party,
    sap_final_cte.search_engine_private_overridden_by_third_party
  ) AS serp_default_private_search_engine_overridden,
  COALESCE(
    serp_final_cte.overridden_by_third_party,
    sap_final_cte.overridden_by_third_party
  ) AS serp_overridden_by_third_party,
  COALESCE(
    serp_final_cte.subsession_start_time,
    sap_final_cte.subsession_start_time
  ) AS serp_subsession_start_time,
  COALESCE(
    serp_final_cte.subsession_end_time,
    sap_final_cte.subsession_end_time
  ) AS serp_subsession_end_time,
  COALESCE(
    serp_final_cte.subsession_counter,
    sap_final_cte.subsession_counter
  ) AS serp_subsession_counter,
  COALESCE(serp_final_cte.experiments, sap_final_cte.experiments) AS serp_experiments,
  COALESCE(
    serp_final_cte.has_adblocker_addon,
    sap_final_cte.has_adblocker_addon
  ) AS serp_has_adblocker_addon,
  COALESCE(
    serp_final_cte.policies_is_enterprise,
    sap_final_cte.policies_is_enterprise
  ) AS serp_policies_is_enterprise,
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
  COALESCE(
    serp_final_cte.profile_age_in_days,
    sap_final_cte.profile_age_in_days
  ) AS serp_profile_age_in_days,
  serp_final_cte.serp_counts,
  -- counts and sums fall back to 0, not NULL, when neither side reported them
  COALESCE(
    serp_final_cte.sessions_started_on_this_day,
    sap_final_cte.sessions_started_on_this_day,
    0
  ) AS serp_sessions_started_on_this_day,
  COALESCE(
    serp_final_cte.active_hours_sum,
    sap_final_cte.active_hours_sum,
    0
  ) AS serp_active_hours_sum,
  -- sap_aggregates_cte casts these integer counters to float64, so cast back to
  -- INT64 to keep the column's declared INTEGER type
  COALESCE(
    serp_final_cte.serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
    CAST(sap_final_cte.scalar_parent_browser_engagement_tab_open_event_count_sum AS INT64),
    0
  ) AS serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
  COALESCE(
    serp_final_cte.serp_scalar_parent_browser_engagement_total_uri_count_sum,
    CAST(sap_final_cte.scalar_parent_browser_engagement_total_uri_count_sum AS INT64),
    0
  ) AS serp_scalar_parent_browser_engagement_total_uri_count_sum,
  COALESCE(
    serp_final_cte.max_concurrent_tab_count_max,
    CAST(sap_final_cte.concurrent_tab_count_max AS INT64),
    0
  ) AS serp_max_concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_v9.serp_final_cte`
  -- FULL OUTER so sap activity with no matching SERP impression is kept.
  -- BigQuery requires a literal `=` in a FULL OUTER JOIN's ON clause, so each key is written
  -- as equality plus an explicit both-NULL match rather than IS NOT DISTINCT FROM.
FULL OUTER JOIN
  `search_derived.search_clients_daily_v9.sap_final_cte`
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
    OR (
      sap_final_cte.source IS NULL
      AND serp_final_cte.serp_search_access_point IS NULL
    )
  )
