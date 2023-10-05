-- TODO: should we run this job with 7 day delay to make sure all data landed (a wider window to be on the safe side).
WITH views_data AS (
  SELECT
    DATE(`date`) AS `date`,
    territory AS country_name,
    SUM(impressions_unique_device) AS views,
  FROM
    app_store.firefox_app_store_territory_source_type_report
  WHERE
    DATE(`date`) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
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
    app_store.firefox_downloads_territory_source_type_report
  WHERE
    DATE(`date`) = DATE_SUB(@submission_date, INTERVAL 7 DAY)
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
  USING
    (`date`, country_name)
  LEFT JOIN
    static.country_names_v1 AS country_names
  ON
    country_names.name = views_data.country_name
),
-- TODO: we may need to consider updating the source for new profiles to be firefox_ios.firefox_ios_clients
-- instead to avoid the other funnel query using a different source.
_new_profiles AS (
  SELECT
    first_seen_date AS `date`,
    first_reported_country AS country,
    COUNT(*) AS new_profiles,
  FROM
    firefox_ios.firefox_ios_clients
  WHERE
    first_seen_date = DATE_SUB(@submission_date, INTERVAL 7 DAY)
    AND channel = "release"
  GROUP BY
    `date`,
    country
)
SELECT
  @submission_date AS submission_date,
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
