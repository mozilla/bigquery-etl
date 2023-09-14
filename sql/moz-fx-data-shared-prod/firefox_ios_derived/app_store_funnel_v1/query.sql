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
  JOIN
    downloads_data
  USING
    (`date`, country_name)
  LEFT OUTER JOIN
    static.country_codes_v1
  ON
    country_name = name
),
new_profiles_and_activations AS (
  SELECT
    first_seen_date AS `date`,
    country,
    SUM(new_profile) AS new_profiles,
    SUM(activated) AS activations,
  FROM
    firefox_ios.new_profile_activation
  WHERE
    submission_date >= '2022-01-01'
    AND first_seen_date >= '2022-01-01'
     -- below filter required due to untrusted devices anomaly,
     -- more information can be found in this bug: https://bugzilla.mozilla.org/show_bug.cgi?id=1846554
    AND NOT (app_display_version = '107.2' AND submission_date >= '2023-02-01')
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
  COALESCE(activations, 0) AS activations,
FROM
  store_stats
FULL OUTER JOIN
  new_profiles_and_activations
USING
  (`date`, country)
