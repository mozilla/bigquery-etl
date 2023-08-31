WITH conversion_counts AS (
  SELECT
    date,
    campaign_id,
    SUM(biddable_app_install_conversions) AS installs,
    SUM(biddable_app_post_install_conversions) AS conversions,
  FROM
    `moz-fx-data-bq-fivetran`.google_ads.campaign_conversions_by_date
  GROUP BY
    date,
    campaign_id
)
SELECT
  date,
  campaign_name,
  campaign_id,
  installs,
  conversions,
FROM
  conversion_counts
JOIN
  `moz-fx-data-shared-prod`.google_ads_derived.campaign_names_map_v1
  USING (campaign_id)
