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
    COALESCE(SUM(fenix_marketing_metrics.dau), 0) AS dau_sum
  FROM
    `moz-fx-data-shared-prod.fenix.marketing_attributable_metrics` AS fenix_marketing_metrics
  WHERE
    adjust_network = "Google Ads ACI"
  GROUP BY
    fenix_marketing_metrics_adjust_campaign,
    date
)
SELECT
  campaigns.name AS campaign_name,
  stats.date AS date,
  -- Total spend per-campaign
  stats.cost_micros AS campaign_spend_in_micros,
  -- Impressions and clicks over time for each campaign (clicks of our ads)
  impressions AS ad_impressions,
  install_dou_metrics.new_profiles_sum AS installs,
  install_dou_metrics.dau_sum AS dous,
  clicks AS revenue_generating_ad_clicks,
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
    clicks = 0
  THEN
    0
  ELSE
    stats.cost_micros / clicks
  END
  AS cost_per_click_micros
FROM
  `moz-fx-data-bq-fivetran`.google_ads.campaign_stats AS stats
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
