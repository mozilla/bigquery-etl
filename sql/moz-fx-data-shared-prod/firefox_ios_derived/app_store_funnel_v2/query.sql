-- https://developer.apple.com/help/app-store-connect/view-sales-and-trends/download-and-view-reports
-- "Time zone: Reports are based on Pacific Time (PT). A day includes transactions that happened from 12:00 a.m. to 11:59 p.m. PT.
-- However, the `date` timestamp field appear to always show midnight meaning if we do timezone conversion
-- we will end up moving all results 1 day back if we attempt conversion to UTC.
-- This is why we are not doing timezone converstions here.
WITH app_store_data AS (
  SELECT
    date_day AS `date`,
    territory_short AS country,
    SUM(impressions_unique_device) AS impressions,
    SUM(total_downloads) AS total_downloads,
    SUM(first_time_downloads) AS first_time_downloads,
    SUM(redownloads) AS redownloads,
  FROM
    `moz-fx-data-shared-prod.app_store_v2_syndicate.apple_store_territory_report`
  WHERE
    DATE(date_day) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND source_type <> 'Institutional Purchase'
    AND app_id = 989804926  -- Filter to only include the Firefox app
  GROUP BY
    ALL
),
_new_profiles AS (
  SELECT
    first_seen_date AS `date`,
    country,
    SUM(new_profiles) AS new_profiles,
  FROM
    `moz-fx-data-shared-prod.firefox_ios.new_profiles`
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND normalized_channel = "release"
  GROUP BY
    ALL
)
SELECT
  @submission_date AS submission_date,
  `date` AS first_seen_date,
  country,
  impressions,
  total_downloads,
  first_time_downloads,
  redownloads,
  COALESCE(new_profiles, 0) AS new_profiles,
FROM
  app_store_data
LEFT JOIN
  _new_profiles
  USING (`date`, country)
