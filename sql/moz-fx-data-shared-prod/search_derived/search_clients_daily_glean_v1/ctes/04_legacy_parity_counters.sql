-- the counters with their client dimensions attached. LEFT JOIN in both cases, so a counter key
-- survives even where the client has no metrics-ping row to describe it: the counters are the
-- reason these rows exist and must not be dropped for want of a dimension. Same shape as the
-- two _is_enterprise_cte joins.
-- In query.sql this CTE sits after clients_with_adblocker_addons_cte rather than with the rest
-- of the legacy block, because a CTE cannot reference one defined below it and the legacy block
-- opens the chain.
-- is_default_browser and overridden_by_third_party are absent by necessity rather than oversight:
-- usage.is_default_browser is not sent in the metrics ping, and overridden_by_third_party is a
-- per-search event extra with no meaning at client-day grain.
WITH legacy_parity_counters_cte AS (
  SELECT
    legacy_counters_agg_cte.*,
    legacy_with_client_info_cte.* EXCEPT (client_id, submission_date),
    -- match the pipelines: a client with no adblocker addon is FALSE, not NULL
    COALESCE(clients_with_adblocker_addons_cte.has_adblocker_addon, FALSE) AS has_adblocker_addon
  FROM
    `search_derived.search_clients_daily_glean_v1.legacy_counters_agg_cte`
  LEFT JOIN
    `search_derived.search_clients_daily_glean_v1.legacy_with_client_info_cte`
    USING (client_id, submission_date)
  LEFT JOIN
    `search_derived.search_clients_daily_glean_v1.clients_with_adblocker_addons_cte`
    USING (client_id, submission_date)
)
-- the file runs standalone, like 03_adblocker.sql; query.sql drops this trailing SELECT and takes
-- the CTE above as an ordinary member of its own WITH chain
SELECT
  *
FROM
  legacy_parity_counters_cte
