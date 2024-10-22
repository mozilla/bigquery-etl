WITH ad_group_names AS (
  SELECT
    campaign_id,
    id AS ad_group_id,
    name AS ad_group_name,
    ROW_NUMBER() OVER (
      PARTITION BY
        id,
        campaign_id
      ORDER BY
        updated_at DESC
    ) = 1 AS is_most_recent_record,
  FROM
    `moz-fx-data-bq-fivetran`.ads_google_mmc.ad_group_history
  QUALIFY
    is_most_recent_record
)
SELECT
  campaigns.account_name,
  campaigns.account_id,
  campaigns.campaign_id,
  campaigns.campaign_name,
  ad_group_names.ad_group_id,
  ad_group_names.ad_group_name,
  mozfun.marketing.parse_ad_group_name(ad_group_names.ad_group_name) AS ad_group_segments,
FROM
  ad_group_names
JOIN
  `moz-fx-data-shared-prod.google_ads_derived.campaigns_v1` AS campaigns
  ON campaigns.campaign_id = ad_group_names.campaign_id
