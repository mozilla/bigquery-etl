-- Query for search_derived.search_dau_aggregates_glean_v1
--
-- Desktop only, by design. The mobile arm of search_dau_aggregates_v1 already reads Glean, so
-- it is not duplicated here. Mobile rows stay in that table, under device = 'mobile'.
--
-- The eligible-country lists encode contract terms and are ported verbatim from
-- search_dau_aggregates_v1. The date on which the list changes is part of that term.
CREATE TEMP FUNCTION google_eligible_country(submission_date DATE, country STRING) AS (
  (submission_date < '2023-12-01' AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
  OR (submission_date >= '2023-12-01' AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
);

-- The DAU population. Read from baseline_active_users, keyed on the Glean client_id, where
-- search_dau_aggregates_v1 read telemetry.desktop_active_users, keyed on the legacy one. One
-- row per client-day, so country is already one value per client-day. app_name excludes
-- Firefox Desktop MozillaOnline and BrowserStack, matching v8's intent.
WITH desktop_dau_data AS (
  SELECT DISTINCT
    'desktop' AS device,
    submission_date,
    country,
    client_id,
    channel AS normalized_channel
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    AND app_name = 'Firefox Desktop'
),
-- What each client searched and had as its default, one row per client, distribution, default
-- engine and engine. Joined to the DAU population on client_id alone, not on country as
-- search_dau_aggregates_v1 did. The DAU and search sides no longer share a geo source, so a
-- country join would silently drop a searching client's search activity wherever the two
-- disagree, counting it as a non-searcher.
desktop_search_data AS (
  SELECT
    submission_date,
    client_id,
    distribution_id,
    -- v8 matched substrings of its raw engine identifier. The provider id is the Glean
    -- equivalent; the display name is not reliable for application-provided engines.
    CASE
      WHEN LOWER(default_search_engine_provider_id) LIKE '%google%'
        THEN 'Google'
      WHEN LOWER(default_search_engine_provider_id) LIKE '%bing%'
        THEN 'Bing'
      WHEN LOWER(default_search_engine_provider_id) LIKE '%ddg%'
        OR LOWER(default_search_engine_provider_id) LIKE '%duckduckgo%'
        THEN 'DuckDuckGo'
      ELSE NULL
    END AS normalized_default_search_engine,
    normalized_engine,
    SUM(sap_counts_total) AS search_count,
    -- v8's ad_click is tagged ad clicks only. ad_click_organic is a separate v8 column and
    -- was never part of this flag, so the organic counters are left out to match.
    SUM(legacy_ad_clicks_tagged_sum) AS ad_click,
    SUM(serp_ad_clicks_tagged_sum) AS ad_click_serp
  FROM
    `moz-fx-data-shared-prod.search_derived.search_clients_daily_glean_v1`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id,
    distribution_id,
    normalized_default_search_engine,
    normalized_engine
),
desktop_by_client_id AS (
  SELECT DISTINCT
    submission_date,
    device,
    normalized_channel,
    country,
    distribution_id,
    normalized_default_search_engine,
    normalized_engine,
    client_id,
    IF(COALESCE(search_count, 0) > 0, 1, 0) AS sap_category,
    IF(COALESCE(ad_click, 0) > 0, 1, 0) AS ad_click_category,
    IF(COALESCE(ad_click_serp, 0) > 0, 1, 0) AS ad_click_category_serp
  FROM
    desktop_dau_data
  LEFT JOIN
    desktop_search_data
    USING (submission_date, client_id)
)
SELECT
  'Google' AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp,
  COUNT(
    DISTINCT IF(
      normalized_default_search_engine = 'Google'
      AND google_eligible_country(submission_date, country),
      client_id,
      NULL
    )
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(
      sap_category > 0
      AND normalized_engine = 'Google'
      AND google_eligible_country(submission_date, country),
      client_id,
      NULL
    )
  ) AS dau_engaged_w_sap,
FROM
  desktop_by_client_id
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp
UNION ALL
SELECT
  'Bing' AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = 'Bing', client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = 'Bing', client_id, NULL)
  ) AS dau_engaged_w_sap,
FROM
  desktop_by_client_id
-- DECISION TO BE FINALIZED. search_dau_aggregates_v1 also excluded clients on the
-- search.acer_cohort list, which is keyed on the legacy client_id and so cannot be joined
-- here. Only the distribution check is carried over, so Bing DAU includes the Acer clients
-- that list would have removed. Accepted for now; the exclusion is to be settled before this
-- table feeds revenue reporting.
WHERE
  distribution_id IS NULL
  OR distribution_id NOT LIKE '%acer%'
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp
UNION ALL
SELECT
  'DuckDuckGo' AS partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp,
  COUNT(
    DISTINCT IF(normalized_default_search_engine = 'DuckDuckGo', client_id, NULL)
  ) AS dau_w_engine_as_default,
  COUNT(
    DISTINCT IF(sap_category > 0 AND normalized_engine = 'DuckDuckGo', client_id, NULL)
  ) AS dau_engaged_w_sap
FROM
  desktop_by_client_id
GROUP BY
  partner,
  submission_date,
  device,
  normalized_channel,
  country,
  distribution_id,
  normalized_default_search_engine,
  normalized_engine,
  sap_category,
  ad_click_category,
  ad_click_category_serp
