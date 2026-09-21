-- the date portion of a client-local timestamp string. Every value it reads carries a trailing
-- offset, so the date is its first ten characters. Converting to UTC would shift it a day for
-- part of the world, which would publish a profile_creation_date one day before the client's own
-- first run. Parsing no time also absorbs the shapes that carry no seconds component.
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
  LOGICAL_AND(ad_blocker_inferred) AS ad_blocker_inferred,
  COUNTIF(
    (is_tagged IS TRUE)
    AND search_access_point IN (
      'follow_on_from_refine_on_incontent_search',
      'follow_on_from_refine_on_serp'
    )
  ) AS follow_on_searches_tagged_count,
  COUNTIF(is_tagged IS TRUE) AS searches_tagged_count,
  COUNTIF(is_tagged IS TRUE AND num_ads_visible > 0) AS searches_with_ads_tagged_count,
  COUNTIF(is_tagged IS FALSE) AS searches_organic_count,
  COUNTIF(is_tagged IS FALSE AND num_ads_visible > 0) AS searches_with_ads_organic_count,
  SUM(CASE WHEN is_tagged IS TRUE THEN num_ad_clicks ELSE 0 END) AS ad_clicks_tagged_sum,
  SUM(CASE WHEN is_tagged IS FALSE THEN num_ad_clicks ELSE 0 END) AS ad_clicks_organic_sum,
  SUM(num_ad_clicks) AS ad_clicks_sum,
  SUM(num_non_ad_link_clicks) AS non_ad_link_clicks_sum,
  SUM(num_other_engagements) AS other_engagements_sum,
  SUM(num_ads_loaded) AS ads_loaded_sum,
  SUM(num_ads_visible) AS ads_visible_sum,
  SUM(num_ads_blocked) AS ads_blocked_sum,
  SUM(num_ads_notshowing) AS ads_notshowing_sum,
  -- Abandonment: an impression with no engagement at teardown. serp_events_v2's validity
  -- filter makes abandonment and engagement mutually exclusive per impression_id, so these
  -- partition cleanly against the engagement counts.
  --
  -- Enumerated rather than pivoted, because abandon_reason has two falsy states meaning
  -- opposite things: NULL is not abandoned, '' is abandoned with the reason absent, which
  -- the client sends for searchTermChanged and pageTypeChanged.
  COUNTIF(abandon_reason = 'navigation') AS abandonments_navigation_count,
  COUNTIF(abandon_reason = 'tab_close') AS abandonments_tab_close_count,
  COUNTIF(abandon_reason = 'window_close') AS abandonments_window_close_count,
  COUNTIF(abandon_reason = '') AS abandonments_reason_absent_count,
  -- catch-all, so no abandonment is ever dropped from the counts. Zero against today's
  -- vocabulary; a non-zero value means the client emits a reason that wants its own column.
  COUNTIF(
    abandon_reason IS NOT NULL
    AND abandon_reason NOT IN ('navigation', 'tab_close', 'window_close', '')
  ) AS abandonments_other_count,
  COUNT(*) AS counts_total,
  MAX(browser_engagement_max_concurrent_tab_count) AS max_concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_glean_v1.serp_base_cte`
GROUP BY
  client_id,
  submission_date,
  provider_id,
  partner_code,
  search_access_point
