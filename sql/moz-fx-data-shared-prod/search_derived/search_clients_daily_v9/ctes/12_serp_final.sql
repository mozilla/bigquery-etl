-- final_serp_cte
SELECT
  serp_events_clients_ad_enterprise_cte.*,
  serp_ad_click_target_cte.ad_click_target,
  serp_aggregates_cte.serp_ad_blocker_inferred,
  serp_aggregates_cte.serp_follow_on_searches_tagged_count,
  serp_aggregates_cte.serp_searches_tagged_count,
  serp_aggregates_cte.serp_searches_organic_count,
  serp_aggregates_cte.serp_with_ads_organic_count,
  serp_aggregates_cte.serp_with_ads_tagged_count,
  serp_aggregates_cte.serp_ad_clicks_tagged_count,
  serp_aggregates_cte.serp_ad_clicks_organic_count,
  serp_aggregates_cte.num_ad_clicks,
  serp_aggregates_cte.num_non_ad_link_clicks,
  serp_aggregates_cte.num_other_engagements,
  serp_aggregates_cte.num_ads_loaded,
  serp_aggregates_cte.num_ads_visible,
  serp_aggregates_cte.num_ads_blocked,
  serp_aggregates_cte.num_ads_notshowing,
  serp_aggregates_cte.profile_age_in_days,
  serp_aggregates_cte.serp_counts,
  serp_aggregates_cte.sessions_started_on_this_day,
  serp_aggregates_cte.active_hours_sum,
  serp_aggregates_cte.serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
  serp_aggregates_cte.serp_scalar_parent_browser_engagement_total_uri_count_sum,
  serp_aggregates_cte.max_concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_v9.serp_events_clients_ad_enterprise_cte`
LEFT JOIN
  `search_derived.search_clients_daily_v9.serp_ad_click_target_cte`
  USING (client_id, submission_date, serp_provider_id, serp_search_access_point)
LEFT JOIN
  `search_derived.search_clients_daily_v9.serp_aggregates_cte`
  USING (client_id, submission_date, serp_provider_id, serp_search_access_point)
