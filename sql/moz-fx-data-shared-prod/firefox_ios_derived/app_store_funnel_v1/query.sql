WITH views_data AS (
  SELECT
    `date`,
    territory AS country_name,
    SUM(impressions_unique_device) AS views,
  FROM
    app_store.firefox_app_store_territory_source_type_report
  WHERE
    `date` >= '2022-01-01'
  GROUP BY
    `date`,
    country_name
),
downloads_data AS (
  SELECT
    `date`,
    territory AS country_name,
    SUM(total_downloads) AS total_downloads,
    SUM(first_time_downloads) AS first_time_downloads,
    SUM(redownloads) AS redownloads,
  FROM
    app_store.firefox_downloads_territory_source_type_report
  WHERE
    `date` >= '2022-01-01'
    AND source_type <> 'Institutional Purchase'
  GROUP BY
    `date`,
    country_name
),
store_stats AS (
  SELECT
    DATE(`date`) AS `date`,
    code AS country,
    views,
    total_downloads,
    first_time_downloads,
    redownloads,
  FROM
    views_data
  FULL OUTER JOIN
    downloads_data
  USING
    (`date`, country_name)
  LEFT OUTER JOIN
    static.country_codes_v1
  ON
    country_name = name
),
_new_profiles AS (
  SELECT
    first_seen_date AS `date`,
    first_reported_country AS country,
    COUNT(*) AS new_profiles,
  FROM
    firefox_ios.firefox_ios_clients
  WHERE
    DATE(submission_timestamp) >= '2022-01-01'
    AND first_seen_date >= '2022-01-01'
    -- TODO: do we need to filter here for "release" channel only?
  GROUP BY
    `date`,
    country
)
SELECT
  `date`,
  country,
  COALESCE(views, 0) AS views,
  COALESCE(total_downloads, 0) AS total_downloads,
  COALESCE(first_time_downloads, 0) AS first_time_downloads,
  COALESCE(redownloads, 0) AS redownloads,
  COALESCE(new_profiles, 0) AS new_profiles,
FROM
  store_stats
FULL OUTER JOIN
  _new_profiles
USING
  (`date`, country)
