CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.google_ads.android_app_campaign_stats`
AS
SELECT
  aacs.`date`,
  aacs.country,
  aacs.impressions,
  aacs.clicks,
  aacs.new_profiles,
  aacs.activated_profiles,
  aacs.spend,
  aacs.lifetime_value,
  cc.country,
  cc.campaign_region
FROM
  `moz-fx-data-shared-prod.google_ads_derived.android_app_campaign_stats_v2` aacs
LEFT JOIN
  (
    SELECT
      `name` AS country,
      CASE
        WHEN code = 'GB'
          THEN 'UK'
        ELSE code
      END AS country_code,
      region_name AS campaign_region
    FROM
      `moz-fx-data-shared-prod.static.country_codes_v1`
  ) cc
  ON aacs.country = cc.country_code
