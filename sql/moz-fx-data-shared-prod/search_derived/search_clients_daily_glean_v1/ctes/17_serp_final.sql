-- final_serp_cte
SELECT
  serp_events_clients_ad_enterprise_cte.*,
  serp_aggregates_cte.ad_click_target,
  serp_aggregates_cte.ad_blocker_inferred,
  serp_aggregates_cte.follow_on_searches_tagged_count,
  serp_aggregates_cte.searches_tagged_count,
  serp_aggregates_cte.searches_organic_count,
  serp_aggregates_cte.searches_with_ads_organic_count,
  serp_aggregates_cte.searches_with_ads_tagged_count,
  serp_aggregates_cte.ad_clicks_tagged_count,
  serp_aggregates_cte.ad_clicks_organic_count,
  serp_aggregates_cte.num_ad_clicks,
  serp_aggregates_cte.non_ad_link_clicks_sum,
  serp_aggregates_cte.other_engagements_sum,
  serp_aggregates_cte.ads_loaded_sum,
  serp_aggregates_cte.ads_visible_sum,
  serp_aggregates_cte.ads_blocked_sum,
  serp_aggregates_cte.ads_notshowing_sum,
  serp_aggregates_cte.counts_total,
  serp_aggregates_cte.max_concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_glean_v1.serp_events_clients_ad_enterprise_cte`
LEFT JOIN
  `search_derived.search_clients_daily_glean_v1.serp_aggregates_cte`
  USING (client_id, submission_date, provider_id, partner_code, search_access_point)
