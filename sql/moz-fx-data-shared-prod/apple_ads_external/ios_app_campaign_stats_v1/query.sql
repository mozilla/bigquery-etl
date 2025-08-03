WITH daily_stats AS (
  SELECT
    date_day AS `date`,
    campaign_name AS campaign,
    campaign_id,
    REGEXP_EXTRACT(
      campaign_name,
      r'Mozilla_(?:FF|Firefox)_ASA_(?:iOSGeoTest_)?([\w]{2,3})_.*'
    ) AS campaign_country_code,
    ad_group_name,
    ad_group_id,
    SUM(spend) AS spend,
    SUM(taps) AS clicks,
    SUM(impressions) AS impressions,
    SUM(total_downloads) AS total_downloads,
  FROM
    `moz-fx-data-shared-prod.apple_ads.ad_group_report`
  WHERE
    {% if is_init() %}
      date_day <= DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY)
    {% else %}
      date_day = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    {% endif %}
  GROUP BY
    `date`,
    campaign_name,
    campaign_id,
    campaign_country_code,
    ad_group_name,
    ad_group_id
),
activations AS (
  SELECT
    clients.first_seen_date AS `date`,
    CAST(REGEXP_EXTRACT(clients.adjust_campaign, r' \((\d+)\)$') AS INT64) AS campaign_id,
    CAST(REGEXP_EXTRACT(adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
    COUNT(DISTINCT ltv.client_id) AS new_profiles,
    COUNTIF(clients.is_activated) AS activated,
    SUM(ltv.lifetime_value) AS lifetime_value,
  FROM
    `moz-fx-data-shared-prod.ltv.firefox_ios_client_ltv` AS ltv
  INNER JOIN
    `moz-fx-data-shared-prod.firefox_ios.new_profile_activation_clients` AS clients
    USING (client_id)
  WHERE
    clients.normalized_channel = "release"
  GROUP BY
    `date`,
    campaign_id,
    ad_group_id
),
retention_aggs AS (
  SELECT
    first_seen_date AS `date`,
    CAST(REGEXP_EXTRACT(adjust_campaign, r' \((\d+)\)$') AS INT64) AS campaign_id,
    CAST(REGEXP_EXTRACT(adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
    SUM(repeat_profiles) AS repeat_users,
    SUM(retained_week_4_new_profiles) AS retained_week_4,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.retention`
  WHERE
    {% if is_init() %}
      metric_date <= CURRENT_DATE
    {% else %}
      metric_date = @submission_date
      AND first_seen_date = @submission_date
    {% endif %}
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
  COALESCE(SUM(impressions), 0) AS impressions,
  COALESCE(SUM(clicks), 0) AS clicks,
  COALESCE(SUM(daily_stats.total_downloads), 0) AS downloads,
  COALESCE(SUM(new_profiles), 0) AS new_profiles,
  COALESCE(SUM(activated), 0) AS activated_profiles,
  COALESCE(SUM(repeat_users), 0) AS repeat_users,
  COALESCE(SUM(retained_week_4), 0) week_4_retained_users,
  COALESCE(SUM(spend), 0) AS spend,
  COALESCE(SUM(lifetime_value), 0) AS lifetime_value,
FROM
  daily_stats
LEFT JOIN
  activations
  USING (`date`, campaign_id, ad_group_id)
LEFT JOIN
  retention_aggs
  USING (`date`, campaign_id, ad_group_id)
GROUP BY
  `date`,
  campaign,
  campaign_id,
  campaign_country_code,
  ad_group_name,
  ad_group_id
