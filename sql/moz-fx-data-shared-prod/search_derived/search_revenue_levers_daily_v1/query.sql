WITH google_data AS (
  SELECT
    submission_date,
    country,
    normalized_engine AS partner,
    'desktop' AS device,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(search_with_ads) AS search_with_ads,
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic,
    SUM(ad_click_organic) AS ad_click_organic,
    SUM(search_with_ads_organic) AS search_with_ads_organic,
    SUM(IF(is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'Google'
    AND (
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
  GROUP BY
    submission_date,
    country,
    partner,
    device
  UNION ALL
  SELECT
    submission_date,
    country,
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
    0 AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'Google'
    AND (
      (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country,
    partner,
    device
),
bing_data AS (
  SELECT
    submission_date,
    country,
    normalized_engine AS partner,
    'desktop' AS device,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(search_with_ads) AS search_with_ads,
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic,
    SUM(ad_click_organic) AS ad_click_organic,
    SUM(search_with_ads_organic) AS search_with_ads_organic,
    SUM(IF(is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'Bing'
    AND is_acer_cohort
  GROUP BY
    submission_date,
    country,
    partner,
    device
  UNION ALL
  SELECT
    submission_date,
    country,
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
    0 AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'Bing'
    AND (
      app_name IN ('Fenix', 'Firefox Preview', 'Focus', 'Focus Android Glean', 'Focus iOS Glean')
      OR (app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    submission_date,
    country,
    partner,
    device
),
ddg_data AS (
  SELECT
    submission_date,
    country,
    normalized_engine AS partner,
    'desktop' AS device,
    SUM(sap) AS sap,
    SUM(tagged_sap) AS tagged_sap,
    SUM(tagged_follow_on) AS tagged_follow_on,
    SUM(search_with_ads) AS search_with_ads,
    SUM(ad_click) AS ad_click,
    SUM(organic) AS organic,
    SUM(ad_click_organic) AS ad_click_organic,
    SUM(search_with_ads_organic) AS search_with_ads_organic,
    SUM(IF(is_sap_monetizable, sap, 0)) AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'DuckDuckGo'
  GROUP BY
    submission_date,
    country,
    partner,
    device
  UNION ALL
  SELECT
    submission_date,
    country,
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
    0 AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'DuckDuckGo'
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
    google_data
  UNION ALL
  SELECT
    *
  FROM
    bing_data
  UNION ALL
  SELECT
    *
  FROM
    ddg_data
),
eligible_markets_dau AS (
  SELECT DISTINCT
    "desktop" AS device,
    submission_date,
    country,
    COUNT(DISTINCT client_id) AS global_eligible_dau,
    COUNT(
      DISTINCT IF(
        (
          (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
          OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
        ),
        client_id,
        NULL
      )
    ) AS google_eligible_dau
  FROM
    `mozdata.telemetry.desktop_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    # not including Mozilla Online
    AND app_name = "Firefox Desktop"
  GROUP BY
    device,
    submission_date,
    country
  UNION ALL
  SELECT DISTINCT
    "mobile" AS device,
    submission_date,
    country,
    COUNT(DISTINCT client_id) AS global_eligible_dau,
    COUNT(
      DISTINCT IF(
        (
          (submission_date < "2023-12-01" AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
          OR (submission_date >= "2023-12-01" AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
        ),
        client_id,
        NULL
      )
    ) AS google_eligible_dau
  FROM
    `mozdata.telemetry.mobile_active_users`
  WHERE
    submission_date = @submission_date
    AND is_dau
    # not including Fenix MozillaOnline, BrowserStack, Klar
    AND app_name IN ("Focus iOS", "Firefox iOS", "Fenix", "Focus Android")
  GROUP BY
    device,
    submission_date,
    country
),
desktop_mobile_search_dau AS (
  SELECT
    submission_date,
    partner,
    device,
    country,
    SUM(dau_w_engine_as_default) AS dau_w_engine_as_default,
    SUM(dau_engaged_w_sap) AS dau_engaged_w_sap
  FROM
    `mozdata.search.search_dau_aggregates`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    partner,
    device,
    country
),
combined_search_dau AS (
  SELECT
    submission_date,
    partner,
    device,
    country,
    CASE
      WHEN partner = "Google"
        THEN google_eligible_dau
      ELSE global_eligible_dau
    END AS dau_eligible_markets,
    dau_w_engine_as_default,
    dau_engaged_w_sap
  FROM
    desktop_mobile_search_dau
  LEFT JOIN
    eligible_markets_dau
    USING (submission_date, device, country)
),
desktop_serp_events AS (
  SELECT
    submission_date,
    `moz-fx-data-shared-prod`.udf.normalize_search_engine(search_engine) AS partner,
    'desktop' AS device,
    normalized_country_code AS country,
    COUNT(impression_id) AS serp_events_sap,
    COUNTIF(is_tagged) AS serp_events_tagged_sap,
    COUNTIF(ad_blocker_inferred) AS serp_events_sap_with_ad_blocker_inferred,
    SUM(num_ad_clicks) AS serp_events_ad_clicks,
    SUM(num_ads_visible) AS serp_events_ads_visible,
    SUM(num_ads_blocked) AS serp_events_ads_blocked
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.serp_events`
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    partner,
    device,
    country
)
SELECT
  cd.submission_date,
  cd.partner,
  cd.device,
  CAST(NULL AS STRING) AS channel,
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
  cd.monetizable_sap
FROM
  combined_search_data cd
LEFT JOIN
  combined_search_dau du
  ON cd.partner = du.partner
  AND cd.submission_date = du.submission_date
  AND cd.country = du.country
  AND cd.device = du.device
LEFT JOIN
  desktop_serp_events dse
  USING (submission_date, device, country, partner)
