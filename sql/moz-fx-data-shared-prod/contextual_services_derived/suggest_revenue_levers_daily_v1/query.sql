-- Query for contextual_services_derived.suggest_revenue_levers_daily_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
-- DAU SHARE BY COUNTRY
-- Used to scale automated global KPI forecasts down to US clients only
-- since pulls global client counts, aggregating over client id and only looking at subset of sample id
WITH client_shares AS (
  SELECT
    country,
    submission_date,
    COUNT(*) AS global_total,
    COUNT(
      CASE
  -- SUGGEST DESKTOP ELIGIBILITY REQUIREMENTS
      WHEN
        normalized_app_name = "Firefox Desktop"
        AND
            -- desktop Suggest experiment start -- 12.5% exposure until 2022-09-21
        (
          submission_date >= "2022-06-07"
          AND browser_version_info.major_version >= 92
          AND browser_version_info.version NOT IN ('92', '92.', '92.0', '92.0.0')
          AND country IN UNNEST(["US"])
          AND locale LIKE "en%"
        )
      THEN
        1
      ELSE
        NULL
      END
    ) AS eligible_clients
  FROM
    telemetry.unified_metrics
  WHERE
    mozfun.bits28.active_in_range(days_seen_bits, 0, 1)
    AND submission_date >= "2022-06-07"
    AND normalized_app_name = "Firefox Desktop"
    AND active_hours_sum > 0
    AND uri_count > 0
    AND sample_id < 10
  GROUP BY
    country,
    submission_date
),
grand_total AS (
  SELECT
    submission_date,
    SUM(global_total) AS global_clients
  FROM
    client_shares
  GROUP BY
    submission_date
),
--- then need metrics by Suggest eligible clients
suggest_clients AS (
  SELECT
    country,
    submission_date,
    COUNT(client_id) AS live_market_dau
  FROM
    telemetry.unified_metrics
  WHERE
    mozfun.bits28.active_in_range(days_seen_bits, 0, 1)
    AND submission_date >= "2022-06-07"
    AND normalized_app_name = "Firefox Desktop"
    AND active_hours_sum > 0
    AND uri_count > 0
    AND browser_version_info.major_version >= 92
    AND browser_version_info.version NOT IN ('92', '92.', '92.0', '92.0.0')
    AND country IN UNNEST(["US"])
    AND locale LIKE "en%"
  GROUP BY
    1,
    2
),
search_clients AS (
  SELECT
    country,
    submission_date,
    client_id,
    SUM(CASE WHEN SOURCE LIKE "urlbar%" THEN sap ELSE 0 END) AS urlbar_search,
    MAX(COALESCE(SOURCE LIKE "urlbar%", FALSE)) AS did_urlbar_search,
  FROM
    search.search_clients_engines_sources_daily
  WHERE
    submission_date >= "2022-06-07"
    AND browser_version_info.major_version >= 92
    AND browser_version_info.version NOT IN ('92', '92.', '92.0', '92.0.0')
    AND country IN UNNEST(["US"])
    AND locale LIKE "en%"
    AND total_uri_count > 0
    AND active_hours_sum > 0
  GROUP BY
    1,
    2,
    3
),
urlbar_clients AS (
  SELECT
    country,
    submission_date,
    SUM(urlbar_search) AS urlbar_search,
    COUNT(DISTINCT client_id) AS urlbar_search_dau,
  FROM
    search_clients
  WHERE
    did_urlbar_search = TRUE
  GROUP BY
    1,
    2
),
desktop_population AS (
  SELECT
    submission_date,
    country,
    "desktop" AS device,
    COUNT(
      CASE
      WHEN
        impression_sponsored_count > 0
        OR impression_nonsponsored_count > 0
      THEN
        client_id
      ELSE
        NULL
      END
    ) AS suggest_exposed_clients,
    SUM(impression_sponsored_count + impression_nonsponsored_count) AS total_impressions,
    SUM(impression_sponsored_count) AS spons_impressions,
    SUM(click_sponsored_count) AS spons_clicks
  FROM
    telemetry.suggest_clients_daily
  WHERE
    submission_date >= "2022-06-07"
  GROUP BY
    submission_date,
    country,
    device
)
SELECT
  country,
  submission_date,
  device,
  eligible_clients / NULLIF(global_clients, 0) AS eligible_share_country,
  live_market_dau,
  urlbar_search_dau / live_market_dau AS perc_engaged_urlbar_search,
  urlbar_search_dau,
  COALESCE(suggest_exposed_clients, 0) / COALESCE(
    urlbar_search_dau,
    0
  ) AS perc_urlbar_dau_exposed_suggest,
  COALESCE(suggest_exposed_clients, 0) AS suggest_exposed_clients,
  urlbar_search,
  total_impressions / urlbar_search AS perc_search_suggest_shown,
  total_impressions,
  spons_impressions / total_impressions AS perc_spons_impressions,
  spons_impressions,
  spons_impressions / urlbar_search_dau AS imp_by_engaged,
  spons_clicks / spons_impressions AS CTR,
  spons_clicks
FROM
  desktop_population
LEFT JOIN
  client_shares
USING
  (country, submission_date)
LEFT JOIN
  suggest_clients
USING
  (submission_date, country)
LEFT JOIN
  urlbar_clients
USING
  (submission_date, country)
LEFT JOIN
  grand_total
USING
  (submission_date)
WHERE
  submission_date = @submission_date
ORDER BY
  submission_date,
  country,
  device
-- to agg to monthly
-- SELECT
--   date_trunc(submission_date, MONTH) as ds_month,
--   country,
--   device,
--   SUM(live_market_dau) as live_market_dau,
--   SUM(urlbar_search_dau)/ SUM(live_market_dau) AS perc_engaged_urlbar_search,
--   SUM(urlbar_search_dau) AS urlbar_search_dau,
--   COALESCE(SUM(suggest_exposed_clients), 0) / COALESCE(
--     SUM(urlbar_search_dau),
--     0
--   ) AS perc_urlbar_dau_exposed_suggest,
--   COALESCE(SUM(suggest_exposed_clients), 0) AS suggest_exposed_clients,
--   SUM(urlbar_search) as urlbar_search,
--   SUM(total_impressions)/ SUM(urlbar_search) AS perc_search_suggest_shown,
--   SUM(total_impressions) AS total_impressions,
--   SUM(spons_impressions) / SUM(total_impressions) AS perc_spons_impressions,
--   SUM(spons_impressions) AS spons_impressions,
--   SUM(spons_impressions) / SUM(urlbar_search_dau) as imp_by_engaged,
--   SUM(spons_clicks)/ SUM(spons_impressions) as CTR,
--   SUM(spons_clicks) AS spons_clicks
-- FROM daily
-- GROUP BY 1, 2, 3
