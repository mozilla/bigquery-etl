WITH historical_store_data AS (
  WITH views_data AS (
    SELECT
      DATE(`date`) AS `date`,
      territory AS country_name,
      SUM(impressions_unique_device) AS views,
    FROM
      `moz-fx-data-shared-prod.app_store.firefox_app_store_territory_source_type_report`
    WHERE
      DATE(`date`) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
      AND source_type <> 'Institutional Purchase'
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
      AND source_type <> 'Institutional Purchase'
      AND app_id = 989804926  -- Filter to only include the Firefox app
    GROUP BY
      ALL
  )
  SELECT
    DATE(`date`) AS `date`,
    country_name,
    COALESCE(views, 0) AS views,
    COALESCE(total_downloads, 0) AS total_downloads,
    COALESCE(first_time_downloads, 0) AS first_time_downloads,
    COALESCE(redownloads, 0) AS redownloads,
  FROM
    views_data
  FULL OUTER JOIN
    downloads_data
    USING (`date`, country_name)
),
app_store_data AS (
  SELECT
    date_day AS `date`,
    territory_long AS country_name,
    SUM(impressions_unique_device) AS views,
    SUM(total_downloads) AS total_downloads,
    SUM(first_time_downloads) AS first_time_downloads,
    SUM(redownloads) AS redownloads,
  FROM
    `moz-fx-data-bq-fivetran.firefox_app_store_v2_apple_store.apple_store__territory_report`
  WHERE
    DATE(date_day) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND source_type <> 'Institutional Purchase'
    AND app_id = 989804926  -- Filter to only include the Firefox app
  GROUP BY
    ALL
),
combine_app_store_data AS (
  SELECT
    `date`,
    country_name,
    views,
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
    views,
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
    views,
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
    COALESCE(SUM(new_profiles), 0) AS new_profiles,
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
  views,
  total_downloads,
  first_time_downloads,
  redownloads,
  new_profiles,
FROM
  normalize_country
LEFT JOIN
  _new_profiles
  USING (`date`, country)
