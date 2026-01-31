WITH source_type_stats AS (
  WITH views_territory AS (
    SELECT
      DATE(`date`) AS `date`,
      app_id,
      territory,
      source_type,
      SUM(impressions) AS impressions,
      SUM(impressions_unique_device) AS impressions_unique_device,
      SUM(page_views) AS page_views,
      SUM(page_views_unique_device) AS page_views_unique_device,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_source_type_report`
    GROUP BY
      `date`,
      app_id,
      territory,
      source_type
  ),
  downloads_territory AS (
    SELECT
      DATE(`date`) AS `date`,
      app_id,
      territory,
      source_type,
      SUM(first_time_downloads) AS first_time_downloads,
      SUM(redownloads) AS redownloads,
      SUM(total_downloads) AS total_downloads,
    FROM
      `moz-fx-data-shared-prod.app_store.downloads_territory_source_type_report`
    GROUP BY
      `date`,
      app_id,
      territory,
      source_type
  ),
  usage_territory AS (
    SELECT
      DATE(`date`) AS `date`,
      app_id,
      territory,
      source_type,
      SUM(active_devices) AS active_devices,
      SUM(active_devices_last_30_days) AS active_devices_last_30_days,
      SUM(deletions) AS deletions,
      SUM(installations) AS installations,
      SUM(sessions) AS sessions,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_usage_territory_source_type_report`
    GROUP BY
      `date`,
      app_id,
      territory,
      source_type
  )
  SELECT
    `date`,
    app_id,
    territory,
    source_type,
    COALESCE(impressions, 0) AS impressions,
    COALESCE(impressions_unique_device, 0) AS impressions_unique_device,
    COALESCE(page_views, 0) AS page_views,
    COALESCE(page_views_unique_device, 0) AS page_views_unique_device,
    COALESCE(first_time_downloads, 0) AS first_time_downloads,
    COALESCE(redownloads, 0) AS redownloads,
    COALESCE(total_downloads, 0) AS total_downloads,
    COALESCE(active_devices, 0) AS active_devices,
    COALESCE(active_devices_last_30_days, 0) AS active_devices_last_30_days,
    COALESCE(deletions, 0) AS deletions,
    COALESCE(installations, 0) AS installations,
    COALESCE(sessions, 0) AS sessions,
  FROM
    views_territory
  FULL OUTER JOIN
    downloads_territory
    USING (`date`, app_id, territory, source_type)
  FULL OUTER JOIN
    usage_territory
    USING (`date`, app_id, territory, source_type)
),
app_referrer_stats AS (
  WITH views_territory AS (
    SELECT
      DATE(`date`) AS `date`,
      app_id,
      territory,
      app_referrer,
      SUM(impressions) AS impressions,
      SUM(impressions_unique_device) AS impressions_unique_device,
      SUM(page_views) AS page_views,
      SUM(page_views_unique_device) AS page_views_unique_device,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_app_referrer_report`
    GROUP BY
      `date`,
      app_id,
      territory,
      app_referrer
  ),
  downloads_territory AS (
    SELECT
      DATE(`date`) AS `date`,
      app_id,
      territory,
      app_referrer,
      SUM(first_time_downloads) AS first_time_downloads,
      SUM(redownloads) AS redownloads,
      SUM(total_downloads) AS total_downloads,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_downloads_territory_app_referrer_report`
    GROUP BY
      `date`,
      app_id,
      territory,
      app_referrer
  ),
  usage_territory AS (
    SELECT
      DATE(`date`) AS `date`,
      app_id,
      territory,
      app_referrer,
      SUM(active_devices) AS active_devices,
      SUM(active_devices_last_30_days) AS active_devices_last_30_days,
      SUM(deletions) AS deletions,
      SUM(installations) AS installations,
      SUM(sessions) AS sessions,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_usage_territory_app_referrer_report`
    GROUP BY
      `date`,
      app_id,
      territory,
      app_referrer
  )
  SELECT
    `date`,
    app_id,
    territory,
    "App Referrer" AS source_type,
    app_referrer,
    COALESCE(impressions, 0) AS impressions,
    COALESCE(impressions_unique_device, 0) AS impressions_unique_device,
    COALESCE(page_views, 0) AS page_views,
    COALESCE(page_views_unique_device, 0) AS page_views_unique_device,
    COALESCE(first_time_downloads, 0) AS first_time_downloads,
    COALESCE(redownloads, 0) AS redownloads,
    COALESCE(total_downloads, 0) AS total_downloads,
    COALESCE(active_devices, 0) AS active_devices,
    COALESCE(active_devices_last_30_days, 0) AS active_devices_last_30_days,
    COALESCE(deletions, 0) AS deletions,
    COALESCE(installations, 0) AS installations,
    COALESCE(sessions, 0) AS sessions,
  FROM
    views_territory
  FULL OUTER JOIN
    downloads_territory
    USING (`date`, app_id, territory, app_referrer)
  FULL OUTER JOIN
    usage_territory
    USING (`date`, app_id, territory, app_referrer)
),
web_referrer_stats AS (
  WITH views_territory AS (
    SELECT
      DATE(web_referrer.`date`) AS `date`,
      web_referrer.app_id,
      web_referrer.territory,
      web_referrer.web_referrer,
      COALESCE(campaign.campaign, "MISSING") AS campaign,
      SUM(COALESCE(campaign.impressions, web_referrer.impressions)) AS impressions,
      SUM(
        COALESCE(campaign.impressions_unique_device, web_referrer.impressions_unique_device)
      ) AS impressions_unique_device,
      SUM(COALESCE(campaign.page_views, web_referrer.page_views)) AS page_views,
      SUM(
        COALESCE(campaign.page_views_unique_device, web_referrer.page_views_unique_device)
      ) AS page_views_unique_device,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_web_referrer_report` AS web_referrer
    LEFT JOIN
      `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_campaign_report` AS campaign
      ON web_referrer.`date` = campaign.`date`
      AND web_referrer.app_id = campaign.app_id
      AND web_referrer.territory = campaign.territory
      AND web_referrer.web_referrer = "mozilla.org"
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
      SUM(
        COALESCE(campaign.first_time_downloads, web_referrer.first_time_downloads)
      ) AS first_time_downloads,
      SUM(COALESCE(campaign.redownloads, web_referrer.redownloads)) AS redownloads,
      SUM(COALESCE(campaign.total_downloads, web_referrer.total_downloads)) AS total_downloads,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_downloads_territory_web_referrer_report` AS web_referrer
    LEFT JOIN
      `moz-fx-data-shared-prod.app_store.firefox_downloads_territory_campaign_report` AS campaign
      ON web_referrer.`date` = campaign.`date`
      AND web_referrer.app_id = campaign.app_id
      AND web_referrer.territory = campaign.territory
      AND web_referrer.web_referrer = "mozilla.org"
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
      SUM(COALESCE(campaign.active_devices, web_referrer.active_devices)) AS active_devices,
      SUM(
        COALESCE(campaign.active_devices_last_30_days, web_referrer.active_devices_last_30_days)
      ) AS active_devices_last_30_days,
      SUM(COALESCE(campaign.deletions, web_referrer.deletions)) AS deletions,
      SUM(COALESCE(campaign.installations, web_referrer.installations)) AS installations,
      SUM(COALESCE(campaign.sessions, web_referrer.sessions)) AS sessions,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_usage_territory_web_referrer_report` AS web_referrer
    LEFT JOIN
      `moz-fx-data-shared-prod.app_store.firefox_usage_territory_campaign_report` AS campaign
      ON web_referrer.`date` = campaign.`date`
      AND web_referrer.app_id = campaign.app_id
      AND web_referrer.territory = campaign.territory
      AND web_referrer.web_referrer = "mozilla.org"
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
    "Web Referrer" AS source_type,
    web_referrer,
    NULLIF(campaign, "MISSING") AS campaign,
    COALESCE(impressions, 0) AS impressions,
    COALESCE(impressions_unique_device, 0) AS impressions_unique_device,
    COALESCE(page_views, 0) AS page_views,
    COALESCE(page_views_unique_device, 0) AS page_views_unique_device,
    COALESCE(first_time_downloads, 0) AS first_time_downloads,
    COALESCE(redownloads, 0) AS redownloads,
    COALESCE(total_downloads, 0) AS total_downloads,
    COALESCE(active_devices, 0) AS active_devices,
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
)
SELECT
  `date`,
  app_id,
  territory,
  source_type,
  app_referrer,
  web_referrer,
  NULLIF(campaign, "MISSING") AS campaign,
  COALESCE(
    web_referrer_stats.impressions,
    app_referrer_stats.impressions,
    source_type_stats.impressions,
    0
  ) AS impressions,
  COALESCE(
    web_referrer_stats.impressions_unique_device,
    app_referrer_stats.impressions_unique_device,
    source_type_stats.impressions_unique_device,
    0
  ) AS impressions_unique_device,
  COALESCE(
    web_referrer_stats.page_views,
    app_referrer_stats.page_views,
    source_type_stats.page_views,
    0
  ) AS page_views,
  COALESCE(
    web_referrer_stats.page_views_unique_device,
    app_referrer_stats.page_views_unique_device,
    source_type_stats.page_views_unique_device,
    0
  ) AS page_views_unique_device,
  COALESCE(
    web_referrer_stats.first_time_downloads,
    app_referrer_stats.first_time_downloads,
    source_type_stats.first_time_downloads,
    0
  ) AS first_time_downloads,
  COALESCE(
    web_referrer_stats.redownloads,
    app_referrer_stats.redownloads,
    source_type_stats.redownloads,
    0
  ) AS redownloads,
  COALESCE(
    web_referrer_stats.total_downloads,
    app_referrer_stats.total_downloads,
    source_type_stats.total_downloads,
    0
  ) AS total_downloads,
  COALESCE(
    web_referrer_stats.active_devices,
    app_referrer_stats.active_devices,
    source_type_stats.active_devices,
    0
  ) AS opt_in_active_devices,
  COALESCE(
    web_referrer_stats.active_devices_last_30_days,
    app_referrer_stats.active_devices_last_30_days,
    source_type_stats.active_devices_last_30_days,
    0
  ) AS opt_in_active_devices_last_30_days,
  COALESCE(
    web_referrer_stats.deletions,
    app_referrer_stats.deletions,
    source_type_stats.deletions,
    0
  ) AS opt_in_deletions,
  COALESCE(
    web_referrer_stats.installations,
    app_referrer_stats.installations,
    source_type_stats.installations,
    0
  ) AS opt_in_installations,
  COALESCE(
    web_referrer_stats.sessions,
    app_referrer_stats.sessions,
    source_type_stats.sessions,
    0
  ) AS opt_in_sessions,
FROM
  source_type_stats
LEFT JOIN
  app_referrer_stats
  USING (`date`, app_id, territory, source_type)
LEFT JOIN
  web_referrer_stats
  USING (`date`, app_id, territory, source_type)
