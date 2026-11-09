-- Status snapshots are reliable from 2026-10-21. A transfer backfill writes the
-- current state into older partitions, so those are not history.
WITH asset_status AS (
  SELECT
    *,
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_asset_status_4813401401`
  WHERE
    _PARTITIONDATE = @submission_date
    AND @submission_date >= '2026-10-21'
    AND campaign_advertising_channel_type = 'MULTI_CHANNEL'
),
campaign_status AS (
  SELECT
    *,
    mozfun.marketing.parse_campaign_name(campaign_name) AS campaign_segments,
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_campaign_status_4813401401`
  WHERE
    _PARTITIONDATE = @submission_date
),
ad_group_status AS (
  SELECT
    *,
  FROM
    `moz-fx-data-marketing-prod.google_ads_dts_mmc.p_ads_ad_group_status_4813401401`
  WHERE
    _PARTITIONDATE = @submission_date
),
-- Fails the query if @submission_date is before history starts, or if any of the
-- three snapshots for @submission_date did not load, so an incomplete partition
-- is not written and the Airflow retries can pick up a late transfer. The LEFT
-- JOIN below always keeps this row, so BigQuery must evaluate the ERROR().
snapshot AS (
  SELECT
    CASE
      WHEN @submission_date < '2026-10-21'
        THEN ERROR(
            CONCAT(CAST(@submission_date AS STRING), ' is before status history start 2026-10-21')
          )
      WHEN (SELECT COUNT(*) FROM asset_status) = 0
        THEN ERROR(CONCAT('No asset_status snapshot for ', CAST(@submission_date AS STRING)))
      WHEN (SELECT COUNT(*) FROM campaign_status) = 0
        THEN ERROR(CONCAT('No campaign_status snapshot for ', CAST(@submission_date AS STRING)))
      WHEN (SELECT COUNT(*) FROM ad_group_status) = 0
        THEN ERROR(CONCAT('No ad_group_status snapshot for ', CAST(@submission_date AS STRING)))
      ELSE @submission_date
    END AS `date`,
),
assets AS (
  SELECT
    asset_status.customer_id,
    asset_status.campaign_id,
    campaign_status.campaign_name,
    campaign_status.campaign_segments,
    asset_status.ad_group_id,
    ad_group_status.ad_group_name,
    asset_status.ad_group_ad_ad_id AS ad_id,
    asset_status.asset_id,
    asset_status.ad_group_ad_asset_view_field_type AS field_type,
    asset_status.ad_group_ad_asset_view_performance_label,
    asset_status.ad_group_ad_asset_view_enabled AS asset_enabled,
    asset_status.ad_group_ad_status AS ad_status,
    ad_group_status.ad_group_status,
    campaign_status.campaign_status,
    asset_status.campaign_advertising_channel_type AS campaign_type,
  FROM
    asset_status
  LEFT JOIN
    campaign_status
    USING (campaign_id)
  LEFT JOIN
    ad_group_status
    USING (ad_group_id)
)
SELECT
  snapshot.`date`,
  assets.customer_id,
  assets.campaign_id,
  assets.campaign_name,
  COALESCE(
    CASE
      UPPER(mozfun.map.get_key(assets.campaign_segments, 'country_code'))
      WHEN 'UK'
        THEN 'GB'
      ELSE UPPER(mozfun.map.get_key(assets.campaign_segments, 'country_code'))
    END,
    'UNKNOWN'
  ) AS market,
  mozfun.map.get_key(assets.campaign_segments, 'region') AS region,
  mozfun.map.get_key(assets.campaign_segments, 'language') AS language,
  assets.ad_group_id,
  assets.ad_group_name,
  assets.ad_id,
  assets.asset_id,
  assets.field_type,
  CASE
    assets.ad_group_ad_asset_view_performance_label
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
  assets.asset_enabled,
  assets.ad_status,
  assets.ad_group_status,
  assets.campaign_status,
  assets.campaign_type,
  COALESCE(
    assets.asset_enabled
    AND assets.ad_group_status = 'ENABLED'
    AND assets.campaign_status = 'ENABLED'
    AND assets.campaign_type = 'MULTI_CHANNEL',
    FALSE
  ) AS is_live,
FROM
  snapshot
LEFT JOIN
  assets
  ON TRUE
