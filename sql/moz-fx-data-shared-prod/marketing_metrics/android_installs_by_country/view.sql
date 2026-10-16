CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.marketing_metrics.android_installs_by_country`
AS
WITH uac_campaigns AS (
  SELECT
    campaign_id,
    -- Google Ads campaign names use UK; Play Store and static.country_codes_v1 use GB.
    -- Unparseable names get a sentinel so installs and spend still join (NULL never matches in USING).
    COALESCE(
      CASE
        UPPER(mozfun.map.get_key(campaign_segments, "country_code"))
        WHEN "UK"
          THEN "GB"
        ELSE UPPER(mozfun.map.get_key(campaign_segments, "country_code"))
      END,
      "UNKNOWN"
    ) AS country_code,
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.campaigns_v2`
),
uac_installs AS (
  SELECT
    conv.`date`,
    c.country_code,
    SUM(conv.installs) AS uac_installs,
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.campaign_conversions_by_date_v1` AS conv
  JOIN
    uac_campaigns AS c
    USING (campaign_id)
  WHERE
    conv.account_name = "Mozilla Firefox UAC"
    AND conv.campaign_name NOT LIKE "%iOS%"
  GROUP BY
    `date`,
    country_code
),
uac_delivery AS (
  SELECT
    stats.`date`,
    c.country_code,
    SUM(stats.spend) AS uac_spend,
    SUM(stats.impressions) AS uac_impressions,
    SUM(stats.clicks) AS uac_clicks,
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.daily_ad_group_stats_v1` AS stats
  JOIN
    uac_campaigns AS c
    USING (campaign_id)
  WHERE
    stats.account_name = "Mozilla Firefox UAC"
    AND stats.campaign_name NOT LIKE "%iOS%"
  GROUP BY
    `date`,
    country_code
),
play_store AS (
  SELECT
    `Date` AS `date`,
    Country AS country_code,
    SUM(Daily_User_Installs) AS play_store_user_installs,
    SUM(Daily_Device_Installs) AS play_store_device_installs,
  FROM
    `moz-fx-data-shared-prod.google_play_store.installs_country`
  WHERE
    Package_Name = "org.mozilla.firefox"
  GROUP BY
    `date`,
    country_code
)
SELECT
  `date`,
  country_code,
  cc.name AS country_name,
  COALESCE(i.uac_installs, 0) AS uac_installs,
  COALESCE(d.uac_spend, 0) AS uac_spend,
  COALESCE(d.uac_impressions, 0) AS uac_impressions,
  COALESCE(d.uac_clicks, 0) AS uac_clicks,
  p.play_store_user_installs,
  p.play_store_device_installs,
FROM
  play_store AS p
FULL OUTER JOIN
  uac_installs AS i
  USING (`date`, country_code)
FULL OUTER JOIN
  uac_delivery AS d
  USING (`date`, country_code)
LEFT JOIN
  `moz-fx-data-shared-prod.static.country_codes_v1` AS cc
  ON cc.code = country_code
