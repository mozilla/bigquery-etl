WITH campaigns AS (
  SELECT DISTINCT
    id,
    name,
    FORMAT("%s (%s)", name, CAST(id AS string)) AS fenix_compatible_campaign_name
  FROM
    `moz-fx-data-bq-fivetran`.google_ads.campaign_history
),
install_dou_metrics AS (
  SELECT
    fenix_marketing_metrics.adjust_campaign AS fenix_marketing_metrics_adjust_campaign,
    fenix_marketing_metrics.submission_date AS date,
    COALESCE(SUM(fenix_marketing_metrics.new_profiles), 0) AS new_profiles_sum,
    COALESCE(SUM(fenix_marketing_metrics.dau), 0) AS dau_sum,
    COALESCE(SUM(fenix_marketing_metrics.ad_clicks), 0) AS revenue_generating_ad_clicks_sum,
  FROM
    `moz-fx-data-shared-prod.fenix.marketing_attributable_metrics` AS fenix_marketing_metrics
  WHERE
    adjust_network = "Google Ads ACI"
  GROUP BY
    fenix_marketing_metrics_adjust_campaign,
    date
),
stats AS (
  SELECT
    id,
    date,
    SUM(cost_micros) as cost_micros,
    SUM(impressions) as impressions,
    SUM(conversions) as conversions,
    SUM(clicks) AS marketing_ad_clicks
  FROM `moz-fx-data-bq-fivetran`.google_ads.campaign_stats AS stats
  GROUP BY id, date
)
SELECT
  campaigns.name AS campaign_name,
  stats.date AS date,
  -- Total spend per-campaign
  stats.cost_micros AS campaign_spend_in_micros,
  -- Impressions and clicks over time for each campaign (clicks of our ads)
  stats.impressions AS ad_impressions,
  install_dou_metrics.new_profiles_sum AS installs,
  install_dou_metrics.dau_sum AS dous,
  stats.conversions as ad_conversions,
  stats.marketing_ad_clicks AS marketing_ad_clicks,
  install_dou_metrics.revenue_generating_ad_clicks_sum AS revenue_generating_ad_clicks,
  -- Cost per-install for each campaign ($/new profiles)
  CASE
  WHEN
    install_dou_metrics.new_profiles_sum = 0
  THEN
    0
  ELSE
    stats.cost_micros / install_dou_metrics.new_profiles_sum
  END
  AS cost_per_install_micros,
  -- Cost per-DOU for each campaign (microunits of local currency/DOU)
  CASE
  WHEN
    install_dou_metrics.dau_sum = 0
  THEN
    0
  ELSE
    stats.cost_micros / install_dou_metrics.dau_sum
  END
  AS cost_per_dou_micros,
  -- Cost per-Ad Click for each campaign (microunits of local currency/Ad Clicks)
  CASE
  WHEN
    marketing_ad_clicks = 0
  THEN
    0
  ELSE
    stats.cost_micros / marketing_ad_clicks
  END
  AS cost_per_marketing_ad_click_micros,
  CASE
  WHEN
    revenue_generating_ad_clicks_sum = 0
  THEN
    0
  ELSE
    stats.cost_micros / revenue_generating_ad_clicks_sum
  END
  AS cost_per_revenue_generating_ad_click_micros
FROM
  stats
JOIN
  campaigns
USING
  (id)
JOIN
  install_dou_metrics
ON
  (stats.date = install_dou_metrics.date)
  AND (
    campaigns.fenix_compatible_campaign_name = install_dou_metrics.fenix_marketing_metrics_adjust_campaign
  )
ORDER BY
  campaign_name,
  date
