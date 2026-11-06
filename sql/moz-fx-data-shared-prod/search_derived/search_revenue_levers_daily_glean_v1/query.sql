-- Query for search_derived.search_revenue_levers_daily_glean_v1
--
-- Glean counterpart of search_revenue_levers_daily_v1. Desktop search comes from
-- search.search_aggregates_glean, desktop DAU from search.search_dau_aggregates_glean and
-- firefox_desktop.baseline_active_users. Mobile already reads Glean in the legacy table, so the
-- mobile arms are carried over unchanged and mobile DAU is still read from
-- search.search_dau_aggregates under device = 'mobile'.
--
-- The eligible-country lists encode contract terms and are ported verbatim from
-- search_revenue_levers_daily_v1. The date on which the list changes is part of that term.
-- How a Google partner_code is monetized: ROW is a RoW code, US is a US code and MINT is a Linux Mint
-- code, all as in today's firefox-b-* codes. NEW is a new Google code
-- (<os>-firefox-<access point>-re, and <os>-firefoxmnt-<access point>-re for Mint), whose rule is not
-- settled yet: it counts as not monetized until it is moved to ROW, US or MINT here. Any other
-- code returns NULL and also counts as not monetized.
CREATE TEMP FUNCTION google_code_class(partner_code STRING) AS (
  CASE
    WHEN partner_code IN ('firefox-b-d', 'firefox-b-e')
      THEN 'ROW'
    WHEN partner_code IN ('firefox-b-1-d', 'firefox-b-1-e')
      THEN 'US'
    WHEN partner_code IN ('firefox-b-lm', 'firefox-b-1-lm')
      THEN 'MINT'
    WHEN REGEXP_CONTAINS(
        partner_code,
        r'^(android|ios|linux|macos|windows)-firefox(mnt)?-[a-z]+-re$'
      )
      THEN 'NEW'
  END
);

CREATE TEMP FUNCTION google_eligible_country(submission_date DATE, country STRING) AS (
  (submission_date < '2023-12-01' AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
  OR (submission_date >= '2023-12-01' AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
);

-- Desktop search measures. The counter_ columns are the series that reconcile to search_aggregates_v8
-- and so to the legacy table; the serp_ equivalents of the two that differ most are published beside
-- them. sap is sap_kpi, which leaves out the runaway-SAP rows the legacy table also left out.
-- Google is limited to eligible countries and Bing to non-Acer distributions, as before. The
-- Acer check here is the distribution only: the legacy search.acer_cohort list is keyed on the
-- legacy client_id and cannot be joined.
WITH desktop_search AS (
  SELECT
    submission_date,
    country,
    normalized_os,
    normalized_engine AS partner,
    'desktop' AS device,
    SUM(sap_kpi) AS sap,
    SUM(counter_searches_tagged_non_follow_on_sum) AS tagged_sap,
    SUM(counter_searches_tagged_follow_on_sum) AS tagged_follow_on,
    SUM(counter_searches_with_ads_tagged_sum) AS search_with_ads,
    SUM(counter_ad_clicks_tagged_sum) AS ad_click,
    SUM(counter_searches_organic_sum) AS organic,
    SUM(counter_ad_clicks_organic_sum) AS ad_click_organic,
    SUM(counter_searches_with_ads_organic_sum) AS search_with_ads_organic,
    -- DEFINITION AWAITING SIGN-OFF from the revenue reviewers. The legacy value comes from
    -- `moz-fx-data-shared-prod.udf.monetized_search`, which reads raw engine codes such as google-b-1-d. The partner_code of a
    -- row names the same variant (firefox-b-1-d), so the rule is rewritten on it, through
    -- google_code_class. Bing and DuckDuckGo are always monetized. Google is monetized on a MINT code
    -- with distribution mint-001, and otherwise outside the excluded territories on a ROW code, or on
    -- a US code in the US only. Any other Google code, including no_code, is not.
    SUM(
      IF(
        CASE
          WHEN normalized_engine IN ('Bing', 'DuckDuckGo')
            THEN TRUE
          WHEN google_code_class(partner_code) = 'MINT'
            AND distribution_id = 'mint-001'
            THEN TRUE
          WHEN country IN ('RU', 'TR', 'KZ', 'BY', 'CN')
            THEN FALSE
          WHEN google_code_class(partner_code) = 'ROW'
            THEN TRUE
          WHEN google_code_class(partner_code) = 'US'
            THEN country = 'US'
          ELSE FALSE
        END,
        sap_kpi,
        0
      )
    ) AS monetizable_sap,
    SUM(serp_searches_with_ads_tagged_count) AS search_with_ads_serp,
    SUM(serp_ad_clicks_tagged_sum) AS ad_click_serp
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates_glean`
  WHERE
    submission_date = @submission_date
    AND normalized_engine IN ('Google', 'Bing', 'DuckDuckGo')
    AND
    CASE
      normalized_engine
      WHEN 'Google'
        THEN google_eligible_country(submission_date, country)
      WHEN 'Bing'
        THEN NOT is_acer_distribution
      ELSE TRUE
    END
  GROUP BY
    submission_date,
    country,
    normalized_os,
    partner,
    device
),
-- Mobile search measures, unchanged from search_revenue_levers_daily_v1. Google and Bing are limited
-- to the listed apps; DuckDuckGo is not. OS and the serp_ columns are not carried for mobile.
mobile_search AS (
  SELECT
    submission_date,
    country,
    CAST(NULL AS STRING) AS normalized_os,
    normalized_engine AS partner,
    'mobile' AS device,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(search_with_ads) AS search_with_ads,
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic,
    SUM(ad_click_organic) AS ad_click_organic,
    0 AS search_with_ads_organic,
    0 AS monetizable_sap,
    CAST(NULL AS INT64) AS search_with_ads_serp,
    CAST(NULL AS INT64) AS ad_click_serp
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
    AND normalized_engine IN ('Google', 'Bing', 'DuckDuckGo')
    AND (normalized_engine != 'Google' OR google_eligible_country(submission_date, country))
    AND (
      normalized_engine = 'DuckDuckGo'
      OR app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country,
    partner,
    device
),
combined_search_data AS (
  SELECT
    *
  FROM
    desktop_search
  UNION ALL
  SELECT
    *
  FROM
    mobile_search
),
-- DAU in the markets a partner is eligible for. Desktop is the Glean DAU population, one row per
-- client-day, limited to Firefox Desktop (which leaves out MozillaOnline and BrowserStack).
-- baseline_active_users.os already holds Windows, Mac, Linux or Other.
eligible_markets_dau AS (
  SELECT
    'desktop' AS device,
    submission_date,
    country,
    os AS normalized_os,
    COUNT(DISTINCT client_id) AS global_eligible_dau,
    COUNT(
      DISTINCT IF(google_eligible_country(submission_date, country), client_id, NULL)
    ) AS google_eligible_dau
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    AND app_name = 'Firefox Desktop'
  GROUP BY
    device,
    submission_date,
    country,
    normalized_os
  UNION ALL
  SELECT
    'mobile' AS device,
    submission_date,
    country,
    CAST(NULL AS STRING) AS normalized_os,
    COUNT(DISTINCT client_id) AS global_eligible_dau,
    COUNT(
      DISTINCT IF(google_eligible_country(submission_date, country), client_id, NULL)
    ) AS google_eligible_dau
  FROM
    `moz-fx-data-shared-prod.telemetry.mobile_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    -- not including Fenix MozillaOnline, BrowserStack, Klar
    AND app_name IN ('Focus iOS', 'Firefox iOS', 'Fenix', 'Focus Android')
  GROUP BY
    device,
    submission_date,
    country
),
-- DAU by search behavior. Desktop comes from the Glean table. Mobile still comes from the legacy
-- table, and the device = 'mobile' filter is load-bearing: without it the legacy desktop rows would
-- be counted as well as the Glean ones.
desktop_mobile_search_dau AS (
  SELECT
    submission_date,
    partner,
    device,
    country,
    normalized_os,
    SUM(dau_w_engine_as_default) AS dau_w_engine_as_default,
    SUM(dau_engaged_w_sap) AS dau_engaged_w_sap
  FROM
    `moz-fx-data-shared-prod.search.search_dau_aggregates_glean`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    partner,
    device,
    country,
    normalized_os
  UNION ALL
  SELECT
    submission_date,
    partner,
    device,
    country,
    CAST(NULL AS STRING) AS normalized_os,
    SUM(dau_w_engine_as_default) AS dau_w_engine_as_default,
    SUM(dau_engaged_w_sap) AS dau_engaged_w_sap
  FROM
    `moz-fx-data-shared-prod.search.search_dau_aggregates`
  WHERE
    submission_date = @submission_date
    AND device = 'mobile'
  GROUP BY
    submission_date,
    partner,
    device,
    country
),
combined_search_dau AS (
  SELECT
    sd.submission_date,
    sd.partner,
    sd.device,
    sd.country,
    sd.normalized_os,
    CASE
      WHEN sd.partner = 'Google'
        THEN emd.google_eligible_dau
      ELSE emd.global_eligible_dau
    END AS dau_eligible_markets,
    sd.dau_w_engine_as_default,
    sd.dau_engaged_w_sap
  FROM
    desktop_mobile_search_dau AS sd
  LEFT JOIN
    eligible_markets_dau AS emd
    ON sd.submission_date = emd.submission_date
    AND sd.device = emd.device
    AND sd.country = emd.country
    AND sd.normalized_os IS NOT DISTINCT FROM emd.normalized_os
)
SELECT
  cd.submission_date,
  cd.partner,
  cd.device,
  cd.normalized_os,
  cd.country,
  du.dau_eligible_markets AS dau,
  du.dau_w_engine_as_default,
  du.dau_engaged_w_sap,
  cd.sap,
  cd.tagged_sap,
  cd.tagged_follow_on,
  cd.search_with_ads,
  cd.ad_click,
  cd.organic,
  cd.ad_click_organic,
  cd.search_with_ads_organic,
  cd.monetizable_sap,
  cd.search_with_ads_serp,
  cd.ad_click_serp
FROM
  combined_search_data AS cd
LEFT JOIN
  combined_search_dau AS du
  ON cd.partner = du.partner
  AND cd.submission_date = du.submission_date
  AND cd.country = du.country
  AND cd.device = du.device
  AND cd.normalized_os IS NOT DISTINCT FROM du.normalized_os
