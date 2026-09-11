-- final_sap_cte
SELECT
  sap_events_clients_ad_enterprise_cte.*,
  sap_aggregates_cte.profile_age_in_days,
  sap_aggregates_cte.sap_counts_total,
  sap_aggregates_cte.concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_glean_v1.sap_events_clients_ad_enterprise_cte`
LEFT JOIN
  `search_derived.search_clients_daily_glean_v1.sap_aggregates_cte`
  USING (client_id, submission_date, normalized_engine, partner_code, source)
-- using(client_id, submission_date, normalized_engine, partner_code, search_access_point) -- rename
