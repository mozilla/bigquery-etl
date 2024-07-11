WITH views_territory AS (
  SELECT
    DATE(web_referrer.`date`) AS `date`,
    web_referrer.app_id,
    web_referrer.territory,
    web_referrer.web_referrer,
    COALESCE(campaign.campaign, "MISSING") AS campaign,
    SUM(campaign.impressions) AS impressions,
    SUM(campaign.impressions_unique_device) AS impressions_unique_device,
    SUM(campaign.page_views) AS page_views,
    SUM(campaign.page_views_unique_device) AS page_views_unique_device,
  FROM
    `moz-fx-data-bq-fivetran.firefox_app_store.app_store_territory_web_referrer_report` AS web_referrer
  LEFT JOIN
    `moz-fx-data-bq-fivetran.firefox_app_store.app_store_territory_campaign_report` AS campaign
    USING (`date`, app_id, territory)
  WHERE
    web_referrer.web_referrer = "mozilla.org"
  GROUP BY
    `date`,
    app_id,
    territory,
    web_referrer,
    campaign
  UNION ALL
  SELECT
    DATE(`date`) AS `date`,
    app_id,
    territory,
    web_referrer,
    "MISSING" AS campaign,
    SUM(impressions) AS impressions,
    SUM(impressions_unique_device) AS impressions_unique_device,
    SUM(page_views) AS page_views,
    SUM(page_views_unique_device) AS page_views_unique_device,
  FROM
    `moz-fx-data-bq-fivetran.firefox_app_store.app_store_territory_web_referrer_report`
  WHERE
    web_referrer <> "mozilla.org"
  GROUP BY
    `date`,
    app_id,
    territory,
    web_referrer,
    campaign
),
downloads_territory AS (
  SELECT
    DATE(web_referrer.`date`) AS `date`,
    web_referrer.app_id,
    web_referrer.territory,
    web_referrer.web_referrer,
    COALESCE(campaign.campaign, "MISSING") AS campaign,
    SUM(campaign.first_time_downloads) AS first_time_downloads,
    SUM(campaign.redownloads) AS redownloads,
    SUM(campaign.total_downloads) AS total_downloads,
  FROM
    `moz-fx-data-bq-fivetran.firefox_app_store.downloads_territory_web_referrer_report` AS web_referrer
  LEFT JOIN
    `moz-fx-data-bq-fivetran.firefox_app_store.downloads_territory_campaign_report` AS campaign
    USING (`date`, app_id, territory)
  WHERE
    web_referrer.web_referrer = "mozilla.org"
  GROUP BY
    `date`,
    app_id,
    territory,
    web_referrer,
    campaign
  UNION ALL
  SELECT
    DATE(`date`) AS `date`,
    app_id,
    territory,
    web_referrer,
    "MISSING" AS campaign,
    SUM(first_time_downloads) AS first_time_downloads,
    SUM(redownloads) AS redownloads,
    SUM(total_downloads) AS total_downloads,
  FROM
    `moz-fx-data-bq-fivetran.firefox_app_store.downloads_territory_web_referrer_report`
  WHERE
    web_referrer <> "mozilla.org"
  GROUP BY
    `date`,
    app_id,
    territory,
    web_referrer,
    campaign
),
usage_territory AS (
  SELECT
    DATE(web_referrer.`date`) AS `date`,
    web_referrer.app_id,
    web_referrer.territory,
    web_referrer.web_referrer,
    COALESCE(campaign.campaign, "MISSING") AS campaign,
    SUM(campaign.active_devices) AS active_devices,
    SUM(campaign.active_devices_last_30_days) AS active_devices_last_30_days,
    SUM(campaign.deletions) AS deletions,
    SUM(campaign.installations) AS installations,
    SUM(campaign.sessions) AS sessions,
  FROM
    `moz-fx-data-bq-fivetran.firefox_app_store.usage_territory_web_referrer_report` AS web_referrer
  LEFT JOIN
    `moz-fx-data-bq-fivetran.firefox_app_store.usage_territory_campaign_report` AS campaign
    USING (`date`, app_id, territory)
  WHERE
    web_referrer.web_referrer = "mozilla.org"
  GROUP BY
    `date`,
    app_id,
    territory,
    web_referrer,
    campaign
  UNION ALL
  SELECT
    DATE(`date`) AS `date`,
    app_id,
    territory,
    web_referrer,
    "MISSING" AS campaign,
    SUM(active_devices) AS active_devices,
    SUM(active_devices_last_30_days) AS active_devices_last_30_days,
    SUM(deletions) AS deletions,
    SUM(installations) AS installations,
    SUM(sessions) AS sessions,
  FROM
    `moz-fx-data-bq-fivetran.firefox_app_store.usage_territory_web_referrer_report`
  WHERE
    web_referrer <> "mozilla.org"
  GROUP BY
    `date`,
    app_id,
    territory,
    web_referrer,
    campaign
)
SELECT
  `date`,
  app_id,
  territory,
  web_referrer,
  NULLIF(campaign, "MISSING") AS campaign,
  COALESCE(impressions, 0) AS impressions,
  COALESCE(impressions_unique_device, 0) AS impressions_unique_device,
  COALESCE(page_views, 0) AS page_views,
  COALESCE(page_views_unique_device, 0) AS page_views_unique_device,
  COALESCE(first_time_downloads, 0) AS first_time_downloads,
  COALESCE(redownloads, 0) AS redownloads,
  COALESCE(total_downloads, 0) AS total_downloads,
  COALESCE(active_devices, 0) AS opt_in_active_devices,
  COALESCE(active_devices_last_30_days, 0) AS active_devices_last_30_days,
  COALESCE(deletions, 0) AS deletions,
  COALESCE(installations, 0) AS installations,
  COALESCE(sessions, 0) AS sessions,
FROM
  views_territory
FULL OUTER JOIN
  downloads_territory
  USING (`date`, app_id, territory, web_referrer, campaign)
FULL OUTER JOIN
  usage_territory
  USING (`date`, app_id, territory, web_referrer, campaign)
