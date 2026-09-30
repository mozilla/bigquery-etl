-- Query for search_derived.search_aggregates_glean_v1
--
-- Collapses client_id out of search_clients_daily_glean_v1 and keeps the dimensions that
-- consumers group by. The Glean analogue of search_aggregates_v8.
--
-- Carries both measurement families. The serp_ columns and sap_counts_total keep the client
-- table's names. The seven legacy_ columns of the client table, which come from the
-- browser.search.* Glean labeled counters, are renamed counter_ here.
-- client_day_country_cte resolves one country per client-day before aggregating. See the
-- comment on that CTE.
WITH client_day_country_cte AS (
  -- The client table stamps each row with the country of the last report behind that row, and
  -- its three source streams are sent at different moments, so a client whose network
  -- location changes during a day (typically a VPN toggling) appears under several countries
  -- on one client-day. v8 gives each client one country per day, so this picks one: the
  -- country on the most rows, ties to the country whose latest row has the latest
  -- ping_end_time, and the country code itself breaks exact ties so the result is
  -- deterministic. ping_end_time is the client's own clock, so it only ranks countries within
  -- a single client, which is all this uses it for.
  SELECT
    client_id,
    submission_date,
    country
  FROM
    (
      SELECT
        client_id,
        submission_date,
        country,
        COUNT(*) AS n_rows,
        MAX(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', ping_end_time)) AS last_end_time
      FROM
        `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
      WHERE
        submission_date = @submission_date
      GROUP BY
        client_id,
        submission_date,
        country
    )
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        n_rows DESC,
        last_end_time DESC,
        country
    ) = 1
)
SELECT
  scd.submission_date,
  scd.normalized_engine,
  scd.partner_code,
  scd.source,
  cdc.country,
  scd.country AS row_country,
  scd.distribution_id,
  scd.locale,
  scd.app_version,
  scd.os,
  scd.os_version,
  scd.channel,
  scd.normalized_channel,
  scd.is_default_browser,
  scd.policies_is_enterprise,
  scd.default_search_engine_display_name,
  scd.default_private_search_engine_display_name,
  scd.home_region,
  -- v8 matched substrings of its raw engine identifier. The provider id is the Glean
  -- equivalent; the display name is not reliable for application-provided engines.
  CASE
    WHEN LOWER(scd.default_search_engine_provider_id) LIKE '%google%'
      THEN 'Google'
    WHEN LOWER(scd.default_search_engine_provider_id) LIKE '%bing%'
      THEN 'Bing'
    WHEN LOWER(scd.default_search_engine_provider_id) LIKE '%ddg%'
      OR LOWER(scd.default_search_engine_provider_id) LIKE '%duckduckgo%'
      THEN 'DuckDuckGo'
    ELSE NULL
  END AS normalized_default_search_engine,
  COUNT(DISTINCT scd.client_id) AS client_count,
  COUNT(DISTINCT IF(scd.has_adblocker_addon, scd.client_id, NULL)) AS clients_with_adblocker_addon,
  SUM(scd.sap_counts_total) AS sap_counts_total,
  SUM(scd.serp_counts_total) AS serp_counts_total,
  SUM(scd.serp_searches_tagged_count) AS serp_searches_tagged_count,
  SUM(scd.serp_follow_on_searches_tagged_count) AS serp_follow_on_searches_tagged_count,
  SUM(scd.serp_searches_organic_count) AS serp_searches_organic_count,
  SUM(scd.serp_searches_with_ads_tagged_count) AS serp_searches_with_ads_tagged_count,
  SUM(scd.serp_searches_with_ads_organic_count) AS serp_searches_with_ads_organic_count,
  SUM(scd.serp_ad_clicks_sum) AS serp_ad_clicks_sum,
  SUM(scd.serp_ad_clicks_tagged_sum) AS serp_ad_clicks_tagged_sum,
  SUM(scd.serp_ad_clicks_organic_sum) AS serp_ad_clicks_organic_sum,
  SUM(scd.serp_ads_loaded_sum) AS serp_ads_loaded_sum,
  SUM(scd.serp_ads_visible_sum) AS serp_ads_visible_sum,
  SUM(scd.serp_ads_blocked_sum) AS serp_ads_blocked_sum,
  SUM(scd.serp_ads_notshowing_sum) AS serp_ads_notshowing_sum,
  SUM(scd.legacy_searches_tagged_non_follow_on_sum) AS counter_searches_tagged_non_follow_on_sum,
  SUM(scd.legacy_searches_tagged_follow_on_sum) AS counter_searches_tagged_follow_on_sum,
  SUM(scd.legacy_searches_organic_sum) AS counter_searches_organic_sum,
  SUM(scd.legacy_searches_with_ads_tagged_sum) AS counter_searches_with_ads_tagged_sum,
  SUM(scd.legacy_searches_with_ads_organic_sum) AS counter_searches_with_ads_organic_sum,
  SUM(scd.legacy_ad_clicks_tagged_sum) AS counter_ad_clicks_tagged_sum,
  SUM(scd.legacy_ad_clicks_organic_sum) AS counter_ad_clicks_organic_sum,
FROM
  `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1` AS scd
JOIN
  client_day_country_cte AS cdc
  ON scd.client_id = cdc.client_id
  AND scd.submission_date = cdc.submission_date
WHERE
  scd.submission_date = @submission_date
GROUP BY
  scd.submission_date,
  scd.normalized_engine,
  scd.partner_code,
  scd.source,
  cdc.country,
  row_country,
  scd.distribution_id,
  scd.locale,
  scd.app_version,
  scd.os,
  scd.os_version,
  scd.channel,
  scd.normalized_channel,
  scd.is_default_browser,
  scd.policies_is_enterprise,
  scd.default_search_engine_display_name,
  scd.default_private_search_engine_display_name,
  scd.home_region,
  normalized_default_search_engine
