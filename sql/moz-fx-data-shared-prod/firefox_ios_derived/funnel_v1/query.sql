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
    SUM(total_downloads) AS downloads,
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
    downloads,
  FROM
    views_data
  JOIN
    downloads_data
  USING
    (`date`, country_name)
  JOIN
    static.country_codes_v1
  ON
    country_name = name
),
activations AS (
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
    AND NOT (
      app_display_version = '107.2'
      AND submission_date >= '2023-02-01'
    ) -- incident filter  #TODO: is there an additional ticket or bug we can reference here?
  GROUP BY
    `date`,
    country
),
first_28_day_retention_stats AS (
  SELECT
    first_seen_date AS `date`,
    country,
    COUNTIF(retention_first_28_days.day_0.active_on_metric_date) AS users_active_on_day_0,
    COUNTIF(active_on_day_1) AS users_active_on_day_1,
    COUNTIF(retention_first_28_days.day_27.active_in_week_0) AS users_active_in_week_0,
    COUNTIF(retention_first_28_days.day_27.active_in_week_1) AS users_active_in_week_1,
    COUNTIF(retention_first_28_days.day_27.active_in_week_2) AS users_active_in_week_2,
    COUNTIF(retention_first_28_days.day_27.active_in_week_3) AS users_active_in_week_3,
  FROM
    firefox_ios_derived.firefox_ios_first_28_day_retention_v1
  GROUP BY
    `date`,
    country
)
SELECT
  `date`,
  country,
  views,
  downloads,
  new_profiles,
  activations,
  users_active_on_day_0,
  users_active_on_day_1,
  users_active_in_week_0,
  users_active_in_week_1,
  users_active_in_week_2,
  users_active_in_week_3,
FROM
  store_stats
JOIN
  activations
USING
  (`date`, country)
JOIN
  first_28_day_retention_stats
USING
  (`date`, country)
