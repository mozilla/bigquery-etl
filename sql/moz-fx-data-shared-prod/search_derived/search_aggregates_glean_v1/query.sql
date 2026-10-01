-- Query for search_derived.search_aggregates_glean_v1
--
-- Glean counterpart of search_aggregates_v8. Drops client_id from
-- search_clients_daily_glean_v1 and keeps the dimensions.
-- The client table's legacy_ counters are renamed counter_ here.
WITH client_day_country_cte AS (
  -- One country per client-day, as in search_clients_daily_v8. A client's rows can carry
  -- different countries, for example when a VPN toggles. Pick the country on the most rows,
  -- then the one with the latest ping_end_time, then the country code.
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
        MAX(mozfun.glean.parse_datetime(ping_end_time)) AS last_end_time
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
),
client_day_dims_cte AS (
  -- One value per client-day for every other dimension. A client's rows can disagree because
  -- each comes from a different source: is_default_browser is only set on SERP and SAP rows,
  -- and app_version changes on release days. Prefer SERP and SAP rows, then the latest
  -- ping_end_time, then the value. IGNORE NULLS skips NULLs, and SAFE_OFFSET returns NULL
  -- when a client is NULL on every row.
  SELECT
    client_id,
    submission_date,
    ARRAY_AGG(
      is_default_browser IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        is_default_browser
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS is_default_browser,
    ARRAY_AGG(
      app_version IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        app_version
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS app_version,
    ARRAY_AGG(
      default_search_engine_provider_id IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        default_search_engine_provider_id
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_search_engine_provider_id,
    ARRAY_AGG(
      default_search_engine_display_name IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        default_search_engine_display_name
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_search_engine_display_name,
    ARRAY_AGG(
      default_private_search_engine_display_name IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        default_private_search_engine_display_name
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS default_private_search_engine_display_name,
    ARRAY_AGG(
      home_region IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        home_region
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS home_region,
    ARRAY_AGG(
      policies_is_enterprise IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        policies_is_enterprise
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS policies_is_enterprise,
    ARRAY_AGG(
      os_version IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        os_version
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS os_version,
    ARRAY_AGG(locale IGNORE NULLS ORDER BY is_event_backed DESC, end_ts DESC, locale LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS locale,
    ARRAY_AGG(
      distribution_id IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        distribution_id
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS distribution_id,
    ARRAY_AGG(channel IGNORE NULLS ORDER BY is_event_backed DESC, end_ts DESC, channel LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS channel,
    ARRAY_AGG(
      normalized_channel IGNORE NULLS
      ORDER BY
        is_event_backed DESC,
        end_ts DESC,
        normalized_channel
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS normalized_channel,
    ARRAY_AGG(os IGNORE NULLS ORDER BY is_event_backed DESC, end_ts DESC, os LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS os
  FROM
    (
      SELECT
        client_id,
        submission_date,
        is_default_browser,
        app_version,
        default_search_engine_provider_id,
        default_search_engine_display_name,
        default_private_search_engine_display_name,
        home_region,
        policies_is_enterprise,
        os_version,
        locale,
        distribution_id,
        channel,
        normalized_channel,
        os,
        (
          COALESCE(sap_counts_total, 0) > 0
          OR COALESCE(serp_counts_total, 0) > 0
        ) AS is_event_backed,
        mozfun.glean.parse_datetime(ping_end_time) AS end_ts
      FROM
        `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
      WHERE
        submission_date = @submission_date
    )
  GROUP BY
    client_id,
    submission_date
)
SELECT
  scd.submission_date,
  scd.normalized_engine,
  scd.partner_code,
  scd.source,
  cdc.country,
  scd.country AS row_country,
  cdd.distribution_id,
  cdd.locale,
  cdd.app_version,
  cdd.os,
  cdd.os_version,
  cdd.channel,
  cdd.normalized_channel,
  cdd.is_default_browser,
  cdd.policies_is_enterprise,
  cdd.default_search_engine_display_name,
  cdd.default_private_search_engine_display_name,
  cdd.home_region,
  -- Same substring match as search_aggregates_v8, on the provider id instead of
  -- default_search_engine.
  CASE
    WHEN LOWER(cdd.default_search_engine_provider_id) LIKE '%google%'
      THEN 'Google'
    WHEN LOWER(cdd.default_search_engine_provider_id) LIKE '%bing%'
      THEN 'Bing'
    WHEN LOWER(cdd.default_search_engine_provider_id) LIKE '%ddg%'
      OR LOWER(cdd.default_search_engine_provider_id) LIKE '%duckduckgo%'
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
LEFT JOIN
  client_day_country_cte AS cdc
  ON scd.client_id = cdc.client_id
  AND scd.submission_date = cdc.submission_date
LEFT JOIN
  client_day_dims_cte AS cdd
  ON scd.client_id = cdd.client_id
  AND scd.submission_date = cdd.submission_date
WHERE
  scd.submission_date = @submission_date
GROUP BY
  scd.submission_date,
  scd.normalized_engine,
  scd.partner_code,
  scd.source,
  cdc.country,
  row_country,
  cdd.distribution_id,
  cdd.locale,
  cdd.app_version,
  cdd.os,
  cdd.os_version,
  cdd.channel,
  cdd.normalized_channel,
  cdd.is_default_browser,
  cdd.policies_is_enterprise,
  cdd.default_search_engine_display_name,
  cdd.default_private_search_engine_display_name,
  cdd.home_region,
  normalized_default_search_engine
