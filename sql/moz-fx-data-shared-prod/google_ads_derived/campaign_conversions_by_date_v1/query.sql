WITH campaigns AS (
  SELECT
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date AS DATE) AS end_date,
    id AS campaign_id,
    name,
  FROM
    `moz-fx-data-bq-fivetran`.google_ads.campaign_history
)
SELECT
  date,
  campaigns.name,
  campaigns.campaign_id,
  SUM(biddable_app_install_conversions) AS installs,
  SUM(biddable_app_post_install_conversions) AS conversions,
FROM
  `moz-fx-data-bq-fivetran`.google_ads.campaign_conversions_by_date
JOIN
  campaigns
ON
  campaign_conversions_by_date.campaign_id = campaigns.campaign_id
  AND campaign_conversions_by_date.date
  BETWEEN campaigns.start_date
  AND campaigns.end_date
GROUP BY
  date,
  campaigns.name,
  campaign_id
