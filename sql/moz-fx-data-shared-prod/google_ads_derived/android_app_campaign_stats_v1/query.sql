WITH daily_stats AS (
  SELECT
    `date`,
    campaign_id,
    CAST(ad_group_id AS INT64) AS ad_group_id,
    spend,
    clicks,
    impressions,
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.daily_ad_group_stats_v1`
  WHERE
    {% if is_init() %}
      `date` <= DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY)
    {% else %}
      `date` = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    {% endif %}
    AND account_name = "Mozilla Firefox UAC"
    AND campaign_name NOT LIKE '%iOS%'
),
activations AS (
  SELECT
    first_seen_date AS `date`,
    ad_group_id,
    COUNTIF(activated) AS activated,
    COUNT(*) AS new_profiles,
    SUM(lifetime_value) AS lifetime_value,
  FROM
    `moz-fx-data-shared-prod.fenix.firefox_android_clients`
  JOIN
    `moz-fx-data-shared-prod.ltv.fenix_client_ltv`
    USING (client_id)
  GROUP BY
    `date`,
    ad_group_id
),
retention_aggs AS (
  SELECT
    first_seen_date AS `date`,
    CAST(REGEXP_EXTRACT(adjust_ad_group, r' \((\d+)\)$') AS INT64) AS ad_group_id,
    SUM(repeat_user) AS repeat_users,
    SUM(retained_week_4) AS retained_week_4
  FROM
    `moz-fx-data-shared-prod.fenix.funnel_retention_week_4`
  WHERE
    {% if is_init() %}
      submission_date <= CURRENT_DATE
    {% else %}
      submission_date = @submission_date
    {% endif %}
  GROUP BY
    `date`,
    ad_group_id
),
by_ad_group_id AS (
  SELECT
    `date`,
    campaign_id,
    ad_group_id,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
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
    USING (`date`, ad_group_id)
  LEFT JOIN
    retention_aggs
    USING (`date`, ad_group_id)
  GROUP BY
    `date`,
    campaign_id,
    ad_group_id
)
SELECT
  `date`,
  campaigns_v2.campaign_name AS campaign,
  CASE
    WHEN LOWER(mozfun.map.get_key(campaigns_v2.campaign_segments, "region")) = "expansion"
      THEN "Expansion"
    ELSE UPPER(mozfun.map.get_key(campaigns_v2.campaign_segments, "region"))
  END AS campaign_region,
  UPPER(
    mozfun.map.get_key(campaigns_v2.campaign_segments, "country_code")
  ) AS campaign_country_code,
  UPPER(mozfun.map.get_key(campaigns_v2.campaign_segments, "language")) AS campaign_language,
  campaigns_v2.campaign_segments,
  ad_groups_v1.ad_group_name AS ad_group,
  ad_groups_v1.ad_group_segments,
  impressions,
  clicks,
  new_profiles,
  activated_profiles,
  repeat_users,
  week_4_retained_users,
  spend,
  lifetime_value,
FROM
  by_ad_group_id
JOIN
  `moz-fx-data-shared-prod.google_ads_derived.ad_groups_v1` AS ad_groups_v1
  USING (ad_group_id)
JOIN
  `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2` AS campaigns_v2
  ON ad_groups_v1.campaign_id = campaigns_v2.campaign_id
