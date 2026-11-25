-- Query for search_derived.search_revenue_levers_daily_glean_v1
--
-- Access-point version of the Glean levers table: the search measures split by access point
-- (source), with no DAU columns. A client counts once in DAU but can use several access points
-- in a day, so DAU cannot be split by access point and is not in this table.
-- Desktop search comes from search.search_aggregates_glean and mobile from
-- search.mobile_search_aggregates.
--
-- The eligible-country lists encode contract terms and are ported verbatim from
-- search_revenue_levers_daily_v1. The date on which the list changes is part of that term.
-- How a Google partner_code is monetized: ROW is a RoW code, US is a US code and MINT is a Linux Mint
-- code, all as in today's firefox-b-* codes. Any other code returns NULL and counts as not
-- monetized, including no_code, the Canonical codes (ubuntu, ubuntu-sn) and any new Google code,
-- until its rule is settled and it is listed here. A data check warns when unlisted codes carry more
-- than 1% of Google desktop SAP.
CREATE TEMP FUNCTION google_code_class(partner_code STRING) AS (
  CASE
    WHEN partner_code IN ('firefox-b-d', 'firefox-b-e')
      THEN 'ROW'
    WHEN partner_code IN ('firefox-b-1-d', 'firefox-b-1-e')
      THEN 'US'
    WHEN partner_code IN ('firefox-b-lm', 'firefox-b-1-lm')
      THEN 'MINT'
  END
);

-- How a Bing or DuckDuckGo partner_code is monetized. The legacy engine strings split each of these
-- partners into variants, and only some are monetized: Glean carries the same split in partner_code.
-- Monetized: Bing MOZI and MOZR, DuckDuckGo ffab and ftsa. Not monetized: no_code (the legacy -addon
-- engines) and lm (the legacy ddg-lm engine). Any other code is not monetized until it is listed
-- here, and a data check warns when unlisted codes carry more than 1% of a partner's desktop SAP.
CREATE TEMP FUNCTION other_partner_code_monetized(
  normalized_engine STRING,
  partner_code STRING
) AS (
  CASE
    WHEN normalized_engine = 'Bing'
      THEN COALESCE(partner_code IN ('MOZI', 'MOZR'), FALSE)
    WHEN normalized_engine = 'DuckDuckGo'
      THEN COALESCE(partner_code IN ('ffab', 'ftsa'), FALSE)
    ELSE FALSE
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
    source,
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
    -- `moz-fx-data-shared-prod.udf.monetized_search`, which reads raw engine strings such as google-b-1-d
    -- or ddg-addon. Glean carries the same information as sap_provider_id plus partner_code, so the rule
    -- is rewritten on partner_code. Bing and DuckDuckGo are monetized on the codes listed in
    -- other_partner_code_monetized. Google is monetized on a MINT code with distribution mint-001, and
    -- otherwise outside the excluded territories on a ROW code, or on a US code in the US only, through
    -- google_code_class. Any code the rule does not list, including no_code, is not monetized.
    SUM(
      IF(
        CASE
          WHEN normalized_engine IN ('Bing', 'DuckDuckGo')
            THEN other_partner_code_monetized(normalized_engine, partner_code)
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
    source,
    partner,
    device
),
-- Mobile search measures, as in search_revenue_levers_daily_v1 with the OS and source columns added.
-- Google and Bing are limited to the listed apps; DuckDuckGo is not. The serp_ columns are not carried
-- for mobile.
mobile_search AS (
  SELECT
    submission_date,
    country,
    os AS normalized_os,
    source,
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
    os,
    source,
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
)
SELECT
  submission_date,
  partner,
  device,
  normalized_os,
  source,
  country,
  sap,
  tagged_sap,
  tagged_follow_on,
  search_with_ads,
  ad_click,
  organic,
  ad_click_organic,
  search_with_ads_organic,
  monetizable_sap,
  search_with_ads_serp,
  ad_click_serp,
  CAST(NULL AS STRING) AS channel
FROM
  combined_search_data
