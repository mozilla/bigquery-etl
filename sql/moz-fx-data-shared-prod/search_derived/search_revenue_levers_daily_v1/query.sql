WITH combined_data AS (
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
    0 AS search_with_ads_organic,
    0 AS monetizable_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates`
  WHERE submission_date = @submission_date
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
   WHERE submission_date = @submission_date
),
desktop_mobile_dau AS (
  SELECT
    submission_date,
    partner,
    device,
    country,
    dau_eligible_markets,
    dau_w_engine_as_default,
    dau_engaged_w_sap
  FROM
    `mozdata.analysis.search_dau_aggregates_ak`
   WHERE submission_date = @submission_date
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
  combined_data cd
LEFT JOIN
  desktop_mobile_dau du 
ON 
  cd.partner = du.partner 
  AND cd.submission_date = du.submission_date 
  AND cd.country = du.country
  AND cd.device = du.device;
