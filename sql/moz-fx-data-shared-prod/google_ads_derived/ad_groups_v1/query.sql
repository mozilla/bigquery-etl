WITH ad_group_names AS (
  SELECT
    campaign_id,
    id AS ad_group_id,
    name AS ad_group_name,
    ROW_NUMBER() OVER (PARTITION BY id, campaign_id ORDER BY updated_at DESC) AS rn,
  FROM
    `moz-fx-data-bq-fivetran`.ads_google_mmc.ad_group_history
  QUALIFY
    rn = 1
)
SELECT
  campaigns.account_name,
  campaigns.account_id,
  campaigns.campaign_id,
  campaigns.campaign_name,
  ad_group_names.ad_group_id,
  ad_group_names.ad_group_name,
FROM
  ad_group_names
JOIN
  google_ads_derived.campaigns_v1 AS campaigns
  ON campaigns.campaign_id = ad_group_names.campaign_id
