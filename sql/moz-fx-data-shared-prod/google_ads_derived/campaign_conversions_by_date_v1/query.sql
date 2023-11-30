WITH conversion_counts AS (
  SELECT
    date,
    customer_id AS account_id,
    campaign_id,
    SUM(biddable_app_install_conversions) AS installs,
    SUM(biddable_app_post_install_conversions) AS conversions,
  FROM
    `moz-fx-data-bq-fivetran`.ads_google_mmc.campaign_conversions_by_date
  GROUP BY
    date,
    account_id,
    campaign_id
)
SELECT
  date,
  account_name,
  account_id,
  campaign_name,
  campaign_id,
  installs,
  conversions,
FROM
  conversion_counts
JOIN
  google_ads_derived.campaign_names_map_v1
USING
  (campaign_id, account_id)
