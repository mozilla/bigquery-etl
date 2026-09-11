-- serp_events_clients_ad_enterprise_cte
SELECT
  serp_events_with_client_info_cte.*,
  -- match v8: clients with no adblocker addon are FALSE, not NULL
  COALESCE(clients_with_adblocker_addons_cte.has_adblocker_addon, FALSE) AS has_adblocker_addon,
  serp_is_enterprise_cte.policies_is_enterprise
FROM
  `search_derived.search_clients_daily_glean_v1.serp_events_with_client_info_cte`
LEFT JOIN
  `search_derived.search_clients_daily_glean_v1.clients_with_adblocker_addons_cte`
  USING (client_id, submission_date)
LEFT JOIN
  `search_derived.search_clients_daily_glean_v1.serp_is_enterprise_cte`
  USING (client_id, submission_date)
