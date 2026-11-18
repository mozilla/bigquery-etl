-- Query for search_derived.search_revenue_levers_dau_glean_v1
--
-- The DAU measures of the Glean levers table, split out of it so the search measures can be split by
-- access point. A client counts once in DAU but can use several access points in a day, so DAU
-- cannot sit in a table split by access point. This table keeps the DAU grain: partner, device,
-- operating system and country.
--
-- Desktop DAU by search behavior comes from search.search_dau_aggregates_glean and eligible DAU from
-- firefox_desktop.baseline_active_users. Mobile still comes from the legacy
-- search.search_dau_aggregates under device = 'mobile', which has no operating system.
--
-- The eligible-country lists encode contract terms and are ported verbatim from
-- search_revenue_levers_daily_v1. The date on which the list changes is part of that term.
CREATE TEMP FUNCTION google_eligible_country(submission_date DATE, country STRING) AS (
  (submission_date < '2023-12-01' AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
  OR (submission_date >= '2023-12-01' AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
);

-- DAU in the markets a partner is eligible for. Desktop is the Glean DAU population, one row per
-- client-day, limited to Firefox Desktop (which leaves out MozillaOnline and BrowserStack).
-- baseline_active_users.os already holds Windows, Mac, Linux or Other.
WITH eligible_markets_dau AS (
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
  submission_date,
  partner,
  device,
  normalized_os,
  country,
  dau_eligible_markets AS dau,
  dau_w_engine_as_default,
  dau_engaged_w_sap
FROM
  combined_search_dau
