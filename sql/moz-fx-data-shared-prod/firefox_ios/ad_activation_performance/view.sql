CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.ad_activation_performance`
AS
WITH client_activation_per_campaign AS (
  SELECT
    first_seen_date,
    CAST(REGEXP_EXTRACT(adjust_campaign, r"\s\(([0-9]+)\)") AS INTEGER) AS campaign_id,
    COUNTIF(is_activated) AS clients_activated,
    COUNT(*) AS new_profiles,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients`
  WHERE
    is_activated IS NOT NULL
  GROUP BY
    first_seen_date,
    campaign_id
),
ad_stats AS (
  SELECT
    date_day,
    campaign_id,
    campaign_name,
    currency,
    SUM(taps) AS taps,
    SUM(new_downloads) AS new_downloads,
    SUM(redownloads) AS redownloads,
    SUM(total_downloads) AS total_downloads,
    SUM(impressions) AS impressions,
    SUM(spend) AS campaign_spend,
  FROM
    `moz-fx-data-shared-prod.apple_ads.campaign_report`
  GROUP BY
    date_day,
    campaign_id,
    campaign_name,
    currency
)
SELECT
  ad_stats.date_day,
  ad_stats.campaign_id,
  ad_stats.campaign_name,
  ad_stats.currency,
  ad_stats.taps,
  ad_stats.new_downloads,
  ad_stats.redownloads,
  ad_stats.total_downloads,
  ad_stats.impressions,
  ad_stats.campaign_spend,
  client_activation_per_campaign.clients_activated,
  client_activation_per_campaign.new_profiles,
  SAFE_DIVIDE(campaign_spend, clients_activated) AS campaign_spend_per_activation,
FROM
  ad_stats
INNER JOIN
  client_activation_per_campaign
  ON ad_stats.date_day = client_activation_per_campaign.first_seen_date
  AND ad_stats.campaign_id = client_activation_per_campaign.campaign_id
