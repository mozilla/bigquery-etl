CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.ad_spend_per_activation`
AS
WITH client_activation_per_campaign AS (
  SELECT
    CAST(REGEXP_EXTRACT(adjust_campaign, r"\s\(([0-9]+)\)") AS INTEGER) AS campaign_id,
    COUNTIF(is_activated) AS clients_activated,
    COUNTIF(NOT is_activated) AS clients_not_activated,
  FROM `moz-fx-data-shared-prod.firefox_ios.firefox_ios_clients`
  GROUP BY campaign_id
),
ad_stats AS (
  SELECT
    campaign_id,
    campaign_name,
    SUM(taps) AS taps,
    SUM(new_downloads) AS new_downloads,
    SUM(redownloads) AS redownloads,
    SUM(total_downloads) AS total_downloads,
    SUM(impressions) AS impressions,
    SUM(spend) AS campaign_spend,
  FROM `moz-fx-data-shared-prod.apple_ads.ad_group_report`
  GROUP BY campaign_id, campaign_name
)

SELECT
  *
FROM ad_stats
INNER JOIN client_activation_per_campaign USING(campaign_id)
