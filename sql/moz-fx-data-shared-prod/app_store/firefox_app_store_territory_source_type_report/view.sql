CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_source_type_report`
AS
-- https://developer.apple.com/help/app-store-connect/view-sales-and-trends/download-and-view-reports
-- "Time zone: Reports are based on Pacific Time (PT). A day includes transactions that happened from 12:00 a.m. to 11:59 p.m. PT.
SELECT
  -- However, this timestamp appear to always show midnight meaning if we do timezone conversion
  -- we will end up moving all results 1 day back if we attempt conversion to UTC.
  DATE(`date`) AS `date`,
  app_id,
  CAST(NULL AS STRING) AS app_name,
  source_type,
  territory,
  impressions,
  impressions_unique_device,
  page_views,
  page_views_unique_device,
  -- These fields did not exist in the previous version.
  CAST(NULL AS INTEGER) AS first_time_downloads,
  CAST(NULL AS INTEGER) AS redownloads,
  CAST(NULL AS INTEGER) AS total_downloads,
FROM
  `moz-fx-data-shared-prod.app_store_syndicate.app_store_territory_source_type_report`
WHERE
  `date` < "2024-01-01"
UNION ALL
-- New connector's data. The schema is a bit different and the new one includes additional metrics:
-- first_time_downloads, redownloads, and total_downloads.
SELECT
  date_day AS `date`,
  app_id,
  app_name,
  source_type,
  territory_long AS territory,
  impressions,
  impressions_unique_device,
  page_views,
  page_views_unique_device,
  first_time_downloads,
  redownloads,
  total_downloads,
FROM
  `moz-fx-data-bq-fivetran.firefox_app_store_v2_apple_store.apple_store__territory_report`
WHERE
  date_day >= "2024-01-01"
