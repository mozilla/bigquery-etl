WITH daily_stats AS (
  SELECT
    date_day AS `date`,
    campaign_name AS campaign,
    campaign_id AS campaign_id,
    REGEXP_EXTRACT(
      campaign_name,
      r'Mozilla_(?:FF|Firefox)_ASA_(?:iOSGeoTest_)?([\w]{2,3})_.*'
    ) AS campaign_country_code,
    ad_group_name,
    ad_group_id,
    DATE_TRUNC(date_day, WEEK) AS week_start,
    FORMAT_DATE("%b", date_day) AS `month`,
    SUM(spend) AS spend,
    SUM(taps) AS clicks,
    SUM(impressions) AS impressions,
    SUM(redownloads) AS redownloads,
    SUM(total_downloads) AS total_downloads
  FROM
    apple_ads.ad_group_report
  GROUP BY
    date_day,
    campaign_name,
    campaign_id,
    REGEXP_EXTRACT(campaign_name, r'Mozilla_(?:FF|Firefox)_ASA_(?:iOSGeoTest_)?([\w]{2,3})_.*'),
    ad_group_name,
    ad_group_id,
    week_start,
    `month`
),
activations AS (
  SELECT
    CAST(REGEXP_EXTRACT(cl.adjust_campaign, r' \((\d+)\)$') AS INT64) AS campaign_id,
    CAST(REGEXP_EXTRACT(adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
    cl.first_seen_date AS date,
    COUNT(DISTINCT ltv.client_id) AS new_profiles,
    COUNTIF(cl.is_activated) AS activated,
    SUM(ltv.lifetime_value) AS lifetime_value
  FROM
    `mozdata.ltv.firefox_ios_client_ltv` ltv
  INNER JOIN
    firefox_ios.firefox_ios_clients cl
    ON ltv.client_id = cl.client_id
    AND ltv.sample_id = cl.sample_id
  WHERE
    cl.channel = "release"
  GROUP BY
    1,
    2,
    3
),
retention_aggs AS (
  SELECT
    first_seen_date AS `date`,
    CAST(REGEXP_EXTRACT(adjust_campaign, r' \((\d+)\)$') AS INT64) AS campaign_id,
    CAST(REGEXP_EXTRACT(adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
    SUM(repeat_user) AS repeat_users,
    SUM(retained_week_4) AS retained_week_4
  FROM
    firefox_ios.funnel_retention_week_4
  GROUP BY
    `date`,
    campaign_id,
    ad_group_id
)
SELECT
  daily_stats.date,
  daily_stats.campaign,
  daily_stats.campaign_id,
  daily_stats.campaign_country_code,
  daily_stats.ad_group_name,
  daily_stats.ad_group_id,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(daily_stats.total_downloads) AS downloads,
  SUM(new_profiles) AS new_profiles,
  SUM(activated) AS activated_profiles,
  SUM(repeat_users) AS repeat_users,
  SUM(retained_week_4) week_4_retained_users,
  SUM(spend) AS spend,
  SUM(lifetime_value) AS lifetime_value,
FROM
  daily_stats
LEFT JOIN
  activations
  USING (date, campaign_id, ad_group_id)
LEFT JOIN
  retention_aggs
  USING (date, campaign_id, ad_group_id)
GROUP BY
  date,
  campaign,
  campaign_id,
  campaign_country_code,
  ad_group_name,
  ad_group_id;
