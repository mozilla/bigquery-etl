-- Google restates recent days, so each run replaces the last
-- @lookback_days + 1 days up to @submission_date.
BEGIN TRANSACTION;

DELETE FROM
  `moz-fx-data-shared-prod.google_ads_derived.ad_asset_daily_performance_v1`
WHERE
  `date`
  BETWEEN DATE_SUB(@submission_date, INTERVAL @lookback_days DAY)
  AND @submission_date;

INSERT INTO
  `moz-fx-data-shared-prod.google_ads_derived.ad_asset_daily_performance_v1`
-- Channel type does not change, so the latest snapshot applies to all dates.
WITH app_campaigns AS (
  SELECT
    campaign_id,
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_campaign_status_4813401401`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY campaign_id ORDER BY _PARTITIONDATE DESC) = 1
    AND campaign_advertising_channel_type = 'MULTI_CHANNEL'
),
ad_groups AS (
  SELECT
    ad_group_id,
    ad_group_name,
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_ad_group_status_4813401401`
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY ad_group_id ORDER BY _PARTITIONDATE DESC) = 1
),
performance AS (
  SELECT
    *,
    mozfun.marketing.parse_campaign_name(campaign_name) AS campaign_segments,
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_ad_group_ad_asset_performance_4813401401`
  JOIN
    app_campaigns
    USING (campaign_id)
  WHERE
    _PARTITIONDATE
    BETWEEN DATE_SUB(@submission_date, INTERVAL @lookback_days DAY)
    AND @submission_date
    AND segments_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL @lookback_days DAY)
    AND @submission_date
)
SELECT
  performance.segments_date AS `date`,
  performance.customer_id,
  performance.campaign_id,
  performance.campaign_name,
  COALESCE(
    CASE
      UPPER(mozfun.map.get_key(performance.campaign_segments, 'country_code'))
      WHEN 'UK'
        THEN 'GB'
      ELSE UPPER(mozfun.map.get_key(performance.campaign_segments, 'country_code'))
    END,
    'UNKNOWN'
  ) AS market,
  mozfun.map.get_key(performance.campaign_segments, 'region') AS region,
  mozfun.map.get_key(performance.campaign_segments, 'language') AS language,
  performance.ad_group_id,
  ad_groups.ad_group_name,
  performance.ad_group_ad_ad_id AS ad_id,
  performance.asset_id,
  performance.asset_name,
  performance.ad_group_ad_asset_view_field_type AS field_type,
  CASE
    performance.ad_group_ad_asset_view_performance_label
    WHEN 'BEST'
      THEN 'Best'
    WHEN 'GOOD'
      THEN 'Good'
    WHEN 'LOW'
      THEN 'Low'
    WHEN 'LEARNING'
      THEN 'Learning'
    WHEN 'PENDING'
      THEN 'Pending'
    ELSE 'Unrated'
  END AS performance_label,
  performance.ad_group_ad_asset_view_enabled AS asset_enabled,
  performance.metrics_impressions AS impressions,
  performance.metrics_clicks AS clicks,
  performance.metrics_cost_micros / 1e6 AS cost,
  performance.metrics_conversions AS conversions,
FROM
  performance
LEFT JOIN
  ad_groups
  USING (ad_group_id);

COMMIT TRANSACTION;
