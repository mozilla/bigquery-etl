-- https://developer.apple.com/help/app-store-connect/view-sales-and-trends/download-and-view-reports
-- "Time zone: Reports are based on Pacific Time (PT). A day includes transactions that happened from 12:00 a.m. to 11:59 p.m. PT.
-- However, the `date` timestamp field appear to always show midnight meaning if we do timezone conversion
-- we will end up moving all results 1 day back if we attempt conversion to UTC.
-- This is why we are not doing timezone converstions here.
WITH historical_store_data AS (
  WITH impression_data AS (
    SELECT
      DATE(`date`) AS `date`,
      territory AS country_name,
      -- Apple used to count pageviews from these sources as impressions as well but no longer do in the new data they're sending back
      -- for compatibility with the new report we only want to count impressions from specific source types?
      SUM(
        CASE
          WHEN source_type IN ('App Referrer', 'Unavailable', 'Web Referrer')
            THEN 0
          ELSE impressions_unique_device
        END
      ) AS impressions,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_source_type_report`
    WHERE
      DATE(`date`) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
      AND LOWER(source_type) <> "institutional purchase"
      AND app_id = 989804926  -- Filter to only include the Firefox app
    GROUP BY
      `date`,
      country_name
  ),
  downloads_data AS (
    SELECT
      DATE(`date`) AS `date`,
      territory AS country_name,
      SUM(total_downloads) AS total_downloads,
      SUM(first_time_downloads) AS first_time_downloads,
      SUM(redownloads) AS redownloads,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_downloads_territory_source_type_report`
    WHERE
      DATE(`date`) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
      AND LOWER(source_type) <> "institutional purchase"
      AND app_id = 989804926  -- Filter to only include the Firefox app
    GROUP BY
      ALL
  )
  SELECT
    DATE(`date`) AS `date`,
    country_name,
    COALESCE(impressions, 0) AS impressions,
    COALESCE(total_downloads, 0) AS total_downloads,
    COALESCE(first_time_downloads, 0) AS first_time_downloads,
    COALESCE(redownloads, 0) AS redownloads,
  FROM
    impression_data
  FULL OUTER JOIN
    downloads_data
    USING (`date`, country_name)
),
app_store_data AS (
  SELECT
    date_day AS `date`,
    territory_long AS country_name,
    SUM(impressions_unique_device) AS impressions,
    SUM(total_downloads) AS total_downloads,
    SUM(first_time_downloads) AS first_time_downloads,
    SUM(redownloads) AS redownloads,
  FROM
    `moz-fx-data-shared-prod.app_store_v2_syndicate.apple_store_territory_report`
  WHERE
    DATE(date_day) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND LOWER(source_type) <> "institutional purchase"
    AND app_id = 989804926  -- Filter to only include the Firefox app
  GROUP BY
    ALL
),
combine_app_store_data AS (
  SELECT
    `date`,
    country_name,
    impressions,
    total_downloads,
    first_time_downloads,
    redownloads,
  FROM
    historical_store_data
  WHERE
    `date` < "2024-01-01"
  UNION ALL
  SELECT
    `date`,
    country_name,
    impressions,
    total_downloads,
    first_time_downloads,
    redownloads,
  FROM
    app_store_data
  WHERE
    `date` >= "2024-01-01"
),
normalize_country AS (
  SELECT
    `date`,
    country_names.code AS country,
    impressions,
    total_downloads,
    first_time_downloads,
    redownloads,
  FROM
    combine_app_store_data
  LEFT JOIN
    `moz-fx-data-shared-prod.static.country_names_v1` AS country_names
    ON combine_app_store_data.country_name = country_names.name
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
  normalize_country
LEFT JOIN
  _new_profiles
  USING (`date`, country)
