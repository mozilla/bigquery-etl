WITH campaigns AS (
  SELECT DISTINCT
    id AS campaign_id,
    name,
  FROM
    `moz-fx-data-bq-fivetran`.google_ads.campaign_history
)
SELECT
  date,
  campaigns.name,
  campaign_id,
  SUM(biddable_app_install_conversions) AS installs,
  SUM(biddable_app_post_install_conversions) AS conversions,
FROM
  `moz-fx-data-bq-fivetran`.google_ads.campaign_conversions_by_date
JOIN
  campaigns
USING
  (campaign_id)
GROUP BY
  date,
  campaigns.name,
  campaign_id
ORDER BY
  date DESC,
  campaigns.name
