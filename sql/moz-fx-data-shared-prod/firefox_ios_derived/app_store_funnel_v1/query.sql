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
    country_names.code AS country,
    views,
    total_downloads,
    first_time_downloads,
    redownloads,
  FROM
    views_data
  FULL OUTER JOIN
    downloads_data
    USING (`date`, country_name)
  LEFT JOIN
    static.country_names_v1 AS country_names
    ON country_names.name = views_data.country_name
),
_new_profiles AS (
  SELECT
    submission_date AS `date`,
    country,
    SUM(new_profiles) AS new_profiles,
  FROM
    firefox_ios.active_users_aggregates
  WHERE
    submission_date >= '2022-01-01'
    AND channel = "release"
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
  USING (`date`, country)
