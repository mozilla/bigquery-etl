-- final_sap_cte
SELECT
  sap_events_clients_ad_enterprise_cte.*,
  sap_aggregates_cte.profile_age_in_days,
  sap_aggregates_cte.sap,
  sap_aggregates_cte.sessions_started_on_this_day,
  sap_aggregates_cte.active_hours_sum,
  sap_aggregates_cte.scalar_parent_browser_engagement_tab_open_event_count_sum,
  sap_aggregates_cte.scalar_parent_browser_engagement_total_uri_count_sum,
  sap_aggregates_cte.concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_v9.sap_events_clients_ad_enterprise_cte`
LEFT JOIN
  `search_derived.search_clients_daily_v9.sap_aggregates_cte`
  USING (client_id, submission_date, normalized_engine, source)
-- using(client_id, submission_date, normalized_engine, partner_code, search_access_point) -- rename
