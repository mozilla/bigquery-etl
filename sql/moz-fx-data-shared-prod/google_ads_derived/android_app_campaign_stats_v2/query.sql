--By day and country, get the total spend, clicks, and impressions for the first seen date (where submission date represents)
WITH daily_stats AS (
  SELECT
    ad_groups_v1.`date`,
    UPPER(mozfun.map.get_key(campaigns_v2.campaign_segments, "country_code")) AS country,
    SUM(spend) AS spend,
    SUM(clicks) AS clicks,
    SUM(impressions) AS impressions,
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.daily_ad_group_stats_v1` ad_groups_v1
  JOIN
    `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2` AS campaigns_v2
    ON ad_groups_v1.campaign_id = campaigns_v2.campaign_id
  WHERE
    ad_groups_v1.date = DATE_SUB(@ltv_recorded_date, INTERVAL 13 DAY)
    AND ad_groups_v1.account_name = "Mozilla Firefox UAC"
    AND ad_groups_v1.campaign_name NOT LIKE '%iOS%'
  GROUP BY
    `date`,
    country
),
--By day and country, get the # of new profiles with a first seen date on that submission date
activations AS (
  SELECT
    np.first_seen_date AS `date`,
    np.country,
    COUNTIF(is_activated) AS activated_profiles,
    COUNT(*) AS new_profiles,
  FROM
    `mozdata.telemetry.mobile_new_profile_clients` np
  LEFT JOIN
    `moz-fx-data-shared-prod.fenix.new_profile_activation_clients` act
    USING (client_id)
  WHERE
    LOWER(np.play_store_attribution_install_referrer_response) LIKE "%gclid%"
    AND np.first_seen_date = DATE_SUB(@ltv_recorded_date, INTERVAL 13 DAY)
  GROUP BY
    `date`,
    country
),
fenix_new_profile_ltv_at_14_days_after_first_seen_date AS (
  SELECT
    *
  FROM
    `mozdata.ltv.fenix_new_profile_ltv`
  WHERE
    submission_date = @ltv_recorded_date
),
revenue AS (
  SELECT
    np.first_seen_date AS `date`,
    np.country,
    SUM(ltv) AS lifetime_value
  FROM
    `mozdata.telemetry.mobile_new_profile_clients` np
  JOIN
    fenix_new_profile_ltv_at_14_days_after_first_seen_date rev
    ON np.client_id = rev.client_id
  WHERE
    LOWER(play_store_attribution_install_referrer_response) LIKE "%gclid%"
  GROUP BY
    `date`,
    country
)
SELECT
  `date`,
  country,
  COALESCE(impressions, 0) AS impressions,
  COALESCE(clicks, 0) AS clicks,
  COALESCE(new_profiles, 0) AS new_profiles,
  COALESCE(activated_profiles, 0) AS activated_profiles,
  COALESCE(spend, 0) AS spend,
  COALESCE(lifetime_value, 0) AS lifetime_value,
FROM
  daily_stats
LEFT JOIN
  activations
  USING (`date`, country)
LEFT JOIN
  revenue
  USING (`date`, country)
