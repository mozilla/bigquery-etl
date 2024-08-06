WITH combined_search_data AS (
  SELECT
    submission_date,
    country,
    normalized_engine AS partner,
    'desktop' AS device,
    sap,
    tagged_sap,
    tagged_follow_on,
    search_with_ads,
    ad_click,
    organic,
    ad_click_organic,
    search_with_ads_organic,
    monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE
    submission_date = @submission_date
  UNION ALL
  SELECT
    submission_date,
    country,
    normalized_engine AS partner,
    'mobile' AS device,
    sap,
    tagged_sap,
    tagged_follow_on,
    search_with_ads,
    ad_click,
    organic,
    ad_click_organic,
    0 AS search_with_ads_organic,
    0 AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.mobile_search_aggregates`
  WHERE
    submission_date = @submission_date
),
eligible_markets_dau AS (
  SELECT
    DISTINCT "desktop" AS device,
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
    1,
    2,
    3
  UNION ALL
  SELECT
    DISTINCT "mobile" AS device,
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
    1,
    2,
    3
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
    1,
    2,
    3,
    4
),
combined_search_dau AS (
  SELECT
    submission_date,
    partner,
    device,
    country,
    CASE
    WHEN
      partner = "Google"
    THEN
      google_eligible_dau
    ELSE
      global_eligible_dau
    END
    AS dau_eligible_markets,
    dau_w_engine_as_default,
    dau_engaged_w_sap
  FROM
    desktop_mobile_search_dau
  LEFT JOIN
    eligible_markets_dau
  USING
    (submission_date, device, country)
)
SELECT
  cd.submission_date,
  cd.partner,
  cd.device,
  NULL AS channel,
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
ON
  cd.partner = du.partner
  AND cd.submission_date = du.submission_date
  AND cd.country = du.country
  AND cd.device = du.device;
