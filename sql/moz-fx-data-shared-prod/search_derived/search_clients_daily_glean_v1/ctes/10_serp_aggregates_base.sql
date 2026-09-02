-- the date portion of a client-local timestamp string. Every value it reads carries a trailing
-- offset, so the date is its first ten characters. Converting to UTC would shift it a day for
-- part of the world, and profile_age_in_days subtracts two of these, so both terms have to be on
-- the same calendar. Parsing no time also absorbs the shapes that carry no seconds component.
CREATE TEMP FUNCTION local_date_of(ts STRING) AS (
  SAFE.PARSE_DATE('%F', SUBSTR(ts, 1, 10))
);

-- serp_aggregates_base_cte
-- the four grain keys arrive resolved from serp_base_cte, so nothing is re-derived here
SELECT
  client_id,
  submission_date,
  provider_id,
  partner_code,
  search_access_point,
  -- serp_aggregates_cte flattens this into ad_click_target. concatenating the group's arrays
  -- leaves the row count alone, so the COUNT(*) and SUMs below stay correct; a
  -- CROSS JOIN UNNEST(ad_components) here would multiply rows and corrupt them
  ARRAY_CONCAT_AGG(ad_components) AS ad_components_all,
  LOGICAL_AND(ad_blocker_inferred) AS serp_ad_blocker_inferred,
  COUNTIF(
    (is_tagged IS TRUE)
    AND search_access_point IN (
      'follow_on_from_refine_on_incontent_search',
      'follow_on_from_refine_on_serp'
    )
  ) AS serp_follow_on_searches_tagged_count,
  COUNTIF(is_tagged IS TRUE) AS serp_searches_tagged_count,
  COUNTIF(is_tagged IS TRUE AND num_ads_visible > 0) AS serp_with_ads_tagged_count,
  COUNTIF(is_tagged IS FALSE) AS serp_searches_organic_count,
  COUNTIF(is_tagged IS FALSE AND num_ads_visible > 0) AS serp_with_ads_organic_count,
  SUM(CASE WHEN is_tagged IS TRUE THEN num_ad_clicks ELSE 0 END) AS serp_ad_clicks_tagged_count,
  SUM(CASE WHEN is_tagged IS FALSE THEN num_ad_clicks ELSE 0 END) AS serp_ad_clicks_organic_count,
  SUM(num_ad_clicks) AS num_ad_clicks,
  SUM(num_non_ad_link_clicks) AS num_non_ad_link_clicks,
  SUM(num_other_engagements) AS num_other_engagements,
  SUM(num_ads_loaded) AS num_ads_loaded,
  SUM(num_ads_visible) AS num_ads_visible,
  SUM(num_ads_blocked) AS num_ads_blocked,
  SUM(num_ads_notshowing) AS num_ads_notshowing,
  -- both terms are client-local dates -- see the SAP copy in sap_aggregates_cte for why
  MAX(UNIX_DATE(local_date_of(subsession_start_time))) - MAX(
    UNIX_DATE(local_date_of(first_run_date))
  ) AS profile_age_in_days,
  COUNT(*) AS serp_counts_total,
  MAX(browser_engagement_max_concurrent_tab_count) AS max_concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_glean_v1.serp_base_cte`
GROUP BY
  client_id,
  submission_date,
  provider_id,
  partner_code,
  search_access_point
