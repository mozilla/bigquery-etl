-- join_sap_serp_cte
SELECT
  -- columns present on both sides are COALESCEd with serp taking precedence;
  -- a serp_ or sap_ prefix means the value comes from that side only
  COALESCE(serp_final_cte.client_id, sap_final_cte.client_id) AS client_id,
  COALESCE(serp_final_cte.submission_date, sap_final_cte.submission_date) AS submission_date,
  COALESCE(serp_final_cte.serp_provider_id, sap_final_cte.normalized_engine) AS provider_id,
  COALESCE(serp_final_cte.partner_code, sap_final_cte.partner_code) AS partner_code,
  COALESCE(
    serp_final_cte.serp_search_access_point,
    sap_final_cte.source
  ) AS search_access_point,
  COALESCE(serp_final_cte.sample_id, sap_final_cte.sample_id) AS sample_id,
  COALESCE(
    serp_final_cte.legacy_telemetry_client_id,
    sap_final_cte.legacy_telemetry_client_id
  ) AS legacy_telemetry_client_id,
  COALESCE(sap_final_cte.sap, 0) AS sap,
  COALESCE(
    serp_final_cte.profile_group_id,
    sap_final_cte.profile_group_id
  ) AS profile_group_id,
  COALESCE(serp_final_cte.country, sap_final_cte.country) AS country,
  COALESCE(serp_final_cte.app_version, sap_final_cte.app_version) AS app_version,
  COALESCE(serp_final_cte.channel, sap_final_cte.channel) AS channel,
  COALESCE(
    serp_final_cte.normalized_channel,
    sap_final_cte.normalized_channel
  ) AS normalized_channel,
  COALESCE(serp_final_cte.locale, sap_final_cte.locale) AS locale,
  COALESCE(serp_final_cte.os, sap_final_cte.os) AS os,
  COALESCE(serp_final_cte.normalized_os, sap_final_cte.normalized_os) AS normalized_os,
  COALESCE(serp_final_cte.os_version, sap_final_cte.os_version) AS os_version,
  COALESCE(
    serp_final_cte.normalized_os_version,
    sap_final_cte.normalized_os_version
  ) AS normalized_os_version,
  serp_final_cte.os_version_major AS serp_os_version_major,
  serp_final_cte.os_version_minor AS serp_os_version_minor,
  COALESCE(
    serp_final_cte.windows_build_number,
    sap_final_cte.windows_build_number
  ) AS windows_build_number,
  COALESCE(serp_final_cte.distribution_id, sap_final_cte.distribution_id) AS distribution_id,
  COALESCE(
    serp_final_cte.profile_creation_date,
    sap_final_cte.profile_creation_date
  ) AS profile_creation_date,
  COALESCE(
    serp_final_cte.region_home_region,
    sap_final_cte.region_home_region
  ) AS region_home_region,
  COALESCE(
    serp_final_cte.usage_is_default_browser,
    sap_final_cte.usage_is_default_browser
  ) AS usage_is_default_browser,
  COALESCE(
    serp_final_cte.search_engine_default_display_name,
    sap_final_cte.search_engine_default_display_name
  ) AS default_search_engine_display_name,
  COALESCE(
    serp_final_cte.search_engine_default_load_path,
    sap_final_cte.search_engine_default_load_path
  ) AS default_search_engine_load_path,
  COALESCE(
    serp_final_cte.search_engine_default_partner_code,
    sap_final_cte.search_engine_default_partner_code
  ) AS default_search_engine_partner_code,
  COALESCE(
    serp_final_cte.search_engine_default_provider_id,
    sap_final_cte.search_engine_default_provider_id
  ) AS default_search_engine_provider_id,
  COALESCE(
    serp_final_cte.search_engine_default_submission_url,
    sap_final_cte.search_engine_default_submission_url
  ) AS default_search_engine_submission_url,
  COALESCE(
    serp_final_cte.search_engine_default_overridden_by_third_party,
    sap_final_cte.search_engine_default_overridden_by_third_party
  ) AS default_search_engine_overridden,
  COALESCE(
    serp_final_cte.search_engine_private_display_name,
    sap_final_cte.search_engine_private_display_name
  ) AS default_private_search_engine_display_name,
  COALESCE(
    serp_final_cte.search_engine_private_load_path,
    sap_final_cte.search_engine_private_load_path
  ) AS default_private_search_engine_load_path,
  COALESCE(
    serp_final_cte.search_engine_private_partner_code,
    sap_final_cte.search_engine_private_partner_code
  ) AS default_private_search_engine_partner_code,
  COALESCE(
    serp_final_cte.search_engine_private_provider_id,
    sap_final_cte.search_engine_private_provider_id
  ) AS default_private_search_engine_provider_id,
  COALESCE(
    serp_final_cte.search_engine_private_submission_url,
    sap_final_cte.search_engine_private_submission_url
  ) AS default_private_search_engine_submission_url,
  COALESCE(
    serp_final_cte.search_engine_private_overridden_by_third_party,
    sap_final_cte.search_engine_private_overridden_by_third_party
  ) AS default_private_search_engine_overridden,
  COALESCE(
    serp_final_cte.overridden_by_third_party,
    sap_final_cte.overridden_by_third_party
  ) AS overridden_by_third_party,
  COALESCE(
    serp_final_cte.subsession_start_time,
    sap_final_cte.subsession_start_time
  ) AS subsession_start_time,
  COALESCE(
    serp_final_cte.subsession_end_time,
    sap_final_cte.subsession_end_time
  ) AS subsession_end_time,
  COALESCE(
    serp_final_cte.subsession_counter,
    sap_final_cte.subsession_counter
  ) AS subsession_counter,
  COALESCE(serp_final_cte.experiments, sap_final_cte.experiments) AS experiments,
  COALESCE(
    serp_final_cte.has_adblocker_addon,
    sap_final_cte.has_adblocker_addon
  ) AS has_adblocker_addon,
  COALESCE(
    serp_final_cte.policies_is_enterprise,
    sap_final_cte.policies_is_enterprise
  ) AS policies_is_enterprise,
  serp_final_cte.ad_click_target AS serp_ad_click_target,
  serp_final_cte.serp_ad_blocker_inferred AS serp_ad_blocker_inferred,
  -- serp-only counts: a sap-only row had no matching SERP rows for that key, so 0 not NULL
  COALESCE(
    serp_final_cte.serp_follow_on_searches_tagged_count,
    0
  ) AS serp_follow_on_searches_tagged_count,
  COALESCE(serp_final_cte.serp_searches_tagged_count, 0) AS serp_searches_tagged_count,
  COALESCE(serp_final_cte.serp_searches_organic_count, 0) AS serp_searches_organic_count,
  COALESCE(serp_final_cte.serp_with_ads_organic_count, 0) AS serp_with_ads_organic_count,
  COALESCE(serp_final_cte.serp_with_ads_tagged_count, 0) AS serp_with_ads_tagged_count,
  COALESCE(serp_final_cte.serp_ad_clicks_tagged_count, 0) AS serp_ad_clicks_tagged_count,
  COALESCE(serp_final_cte.serp_ad_clicks_organic_count, 0) AS serp_ad_clicks_organic_count,
  COALESCE(serp_final_cte.num_ad_clicks, 0) AS serp_num_ad_clicks,
  COALESCE(serp_final_cte.num_non_ad_link_clicks, 0) AS serp_num_non_ad_link_clicks,
  COALESCE(serp_final_cte.num_other_engagements, 0) AS serp_num_other_engagements,
  COALESCE(serp_final_cte.num_ads_loaded, 0) AS serp_num_ads_loaded,
  COALESCE(serp_final_cte.num_ads_visible, 0) AS serp_num_ads_visible,
  COALESCE(serp_final_cte.num_ads_blocked, 0) AS serp_num_ads_blocked,
  COALESCE(serp_final_cte.num_ads_notshowing, 0) AS serp_num_ads_notshowing,
  COALESCE(
    serp_final_cte.profile_age_in_days,
    sap_final_cte.profile_age_in_days
  ) AS profile_age_in_days,
  COALESCE(serp_final_cte.serp_counts, 0) AS serp_counts,
  -- counts and sums fall back to 0, not NULL, when neither side reported them
  COALESCE(
    serp_final_cte.sessions_started_on_this_day,
    sap_final_cte.sessions_started_on_this_day,
    0
  ) AS sessions_started_on_this_day,
  COALESCE(
    serp_final_cte.active_hours_sum,
    sap_final_cte.active_hours_sum,
    0
  ) AS active_hours_sum,
  -- sap_aggregates_cte casts these integer counters to float64, so cast back to
  -- INT64 to keep the column's declared INTEGER type
  COALESCE(
    serp_final_cte.serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
    CAST(sap_final_cte.scalar_parent_browser_engagement_tab_open_event_count_sum AS INT64),
    0
  ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
  COALESCE(
    serp_final_cte.serp_scalar_parent_browser_engagement_total_uri_count_sum,
    CAST(sap_final_cte.scalar_parent_browser_engagement_total_uri_count_sum AS INT64),
    0
  ) AS scalar_parent_browser_engagement_total_uri_count_sum,
  COALESCE(
    serp_final_cte.max_concurrent_tab_count_max,
    CAST(sap_final_cte.concurrent_tab_count_max AS INT64),
    0
  ) AS max_concurrent_tab_count_max
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
