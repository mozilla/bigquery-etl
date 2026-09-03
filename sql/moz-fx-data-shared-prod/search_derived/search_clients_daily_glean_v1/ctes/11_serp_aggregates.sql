-- serp_aggregates_cte
-- Flattens ad_components_all into the ad_click_target string. ARRAY_CONCAT_AGG has to land as a
-- column in serp_aggregates_base_cte before it can be unnested, because BigQuery rejects an
-- aggregate inside UNNEST. Reads the base CTE, not serp_events, so there is no second scan.
-- A key whose ad_components are all empty concatenates to an empty array, which UNNESTs to no
-- rows and leaves STRING_AGG null.
-- ORDER BY makes the concatenation order deterministic (STRING_AGG is arbitrary otherwise)
SELECT
  * EXCEPT (ad_components_all),
  (
    SELECT
      STRING_AGG(DISTINCT ad_component.component, ', ' ORDER BY ad_component.component)
    FROM
      UNNEST(ad_components_all) AS ad_component
  ) AS ad_click_target
FROM
  `search_derived.search_clients_daily_glean_v1.serp_aggregates_base_cte`
