CREATE TEMP FUNCTION safe_parse_timestamp(ts string) AS (
  COALESCE(
        -- full datetime with offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", ts),
        -- date + offset (no time)
    SAFE.PARSE_TIMESTAMP("%F%Ez", ts),
        -- datetime with space before offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", REGEXP_REPLACE(ts, r"(\+|\-)(\d{2}):(\d{2})", "\\1\\2\\3"))
  )
);

CREATE TEMP FUNCTION to_utc_string(ts STRING) AS (
  FORMAT_TIMESTAMP(
    '%F %T UTC',  -- desired output format
    SAFE.PARSE_TIMESTAMP('%FT%H:%M:%E*S%Ez', ts),
    'UTC'
  )
);

-- serp_aggregates_cte
SELECT
  glean_client_id AS client_id,
  submission_date,
  `moz-fx-data-shared-prod.udf.normalize_search_engine`(
    search_engine
  ) AS serp_provider_id, -- this is engine
  sap_source AS serp_search_access_point,
  LOGICAL_AND(ad_blocker_inferred) AS serp_ad_blocker_inferred,
  COUNTIF(
    (is_tagged IS TRUE)
    AND (
      sap_source = 'follow_on_from_refine_on_incontent_search'
      OR sap_source = 'follow_on_from_refine_on_serp'
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
  MAX(UNIX_DATE(DATE(to_utc_string(subsession_start_time)))) - MAX(
    UNIX_DATE(DATE(safe_parse_timestamp(first_run_date)))
  ) AS profile_age_in_days,
  COUNT(*) AS serp_counts,
  COUNTIF(subsession_counter = 1) AS sessions_started_on_this_day,
  SUM(browser_engagement_active_ticks / (3600 / 5)) AS active_hours_sum,
  SUM(
    browser_engagement_tab_open_event_count
  ) AS serp_scalar_parent_browser_engagement_tab_open_event_count_sum,
  SUM(browser_engagement_uri_count) AS serp_scalar_parent_browser_engagement_total_uri_count_sum,
  MAX(browser_engagement_max_concurrent_tab_count) AS max_concurrent_tab_count_max
FROM
  `mozdata.firefox_desktop.serp_events` -- serp_events_v2 doesn't have the aggregated fields like `num_ads_visible`
WHERE
  submission_date
  BETWEEN '2025-06-25'
  AND '2025-09-25'
  AND sample_id = 0
-- submission_date = @submission_date
GROUP BY
  client_id,
  submission_date,
  serp_provider_id,
  serp_search_access_point
