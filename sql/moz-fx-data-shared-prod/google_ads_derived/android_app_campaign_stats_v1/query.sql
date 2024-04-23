WITH daily_stats AS (
  SELECT
    date,
    campaign_id,
    CAST(ad_group_id AS INT64) AS ad_group_id,
    spend,
    clicks,
    impressions,
  FROM
    google_ads_derived.daily_ad_group_stats_v1
  WHERE
    date >= '2022-12-01'
    AND account_name = "Mozilla Firefox UAC"
    AND campaign_name NOT LIKE '%iOS%'
),
activations AS (
  SELECT
    first_seen_date AS date,
    CAST(REGEXP_EXTRACT(adjust_campaign, r' \((\d+)\)$') AS INT64) as campaign_id,
    CAST(REGEXP_EXTRACT(adjust_ad_group, r' \((\d+)\)$') AS INT64) as ad_group_id,
    COUNTIF(activated) AS activated,
    COUNT(*) AS new_profiles,
    SUM(lifetime_value) AS lifetime_value,
  FROM
    fenix_derived.firefox_android_clients_v1
  JOIN
    mozdata.ltv.fenix_client_ltv
    USING (client_id)
  WHERE
    first_seen_date >= '2022-12-01'
  GROUP BY
    date,
    campaign_id,
    ad_group_id
)
SELECT
  date,
  campaigns_v1.campaign_name AS campaign,
  campaigns_v1.campaign_region,
  campaigns_v1.campaign_country_code,
  campaigns_v1.campaign_language,
  ad_groups_v1.ad_group_name AS ad_group,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(new_profiles) AS new_profiles,
  SUM(activated) AS activated_profiles,
  SUM(spend) AS spend,
  SUM(lifetime_value) AS lifetime_value,
FROM
  daily_stats
LEFT JOIN
  activations
  USING (date, campaign_id, ad_group_id)
JOIN
  google_ads_derived.campaigns_v1
  USING (campaign_id)
JOIN
  google_ads_derived.ad_groups_v1
  USING (campaign_id, ad_group_id)
GROUP BY
  date,
  campaign,
  campaign_region,
  campaign_country_code,
  campaign_language,
  ad_group
