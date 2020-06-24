WITH countries AS (
  SELECT
    code,
    name
  FROM
    `moz-fx-data-shared-prod.static.country_names_v1`
),
sample AS (
  SELECT
    submission_date,
    DATE_TRUNC(submission_date, WEEK(MONDAY)) AS week_start,
    EXTRACT(DAYOFWEEK FROM submission_date) = 1 AS is_last_day_of_week,
    days_since_seen,
    COALESCE(cn.name, country_group) AS country_name,
    subsession_hours_sum,
    days_seen_bits,
    days_created_profile_bits,
    client_id,
    app_version
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`,
    UNNEST([country, 'Worldwide']) AS country_group
  LEFT JOIN
    countries AS cn
  ON
    cn.code = country_group
  WHERE
    COALESCE(cn.name, country_group) IN (
      'Worldwide',
      'Brazil',
      'China',
      'France',
      'Germany',
      'India',
      'Indonesia',
      'Italy',
      'Poland',
      'Russia',
      'United States'
    )
    -- we need the whole week for daily_usage metric
    -- others can look at just the last day (Sunday, see `is_last_day_of_week` above)
    AND submission_date >= @submission_date
    AND submission_date < DATE_ADD(@submission_date, INTERVAL 7 DAY)
    AND subsession_hours_sum < 24 --remove outliers
),
mau AS (
  SELECT
    week_start,
    country_name,
    count(*) AS MAU
  FROM
    sample
  WHERE
    is_last_day_of_week
    AND days_since_seen < 28
  GROUP BY
    week_start,
    country_name
),
avg_daily_usage_by_user AS (
  SELECT
    client_id,
    country_name,
    avg(subsession_hours_sum) AS avg_hours_usage_daily_per_user,
    week_start
  FROM
    sample
  WHERE
    days_since_seen = 0
  GROUP BY
    client_id,
    country_name,
    week_start
  HAVING
    avg_hours_usage_daily_per_user < 24 --remove outliers
),
daily_usage AS (
  SELECT
    country_name,
    avg(avg_hours_usage_daily_per_user) AS avg_hours_usage_daily,
    week_start
  FROM
    avg_daily_usage_by_user
  GROUP BY
    country_name,
    week_start
),
intensity AS (
  SELECT
    week_start,
    country_name,
    SAFE_DIVIDE(
      SUM(`moz-fx-data-shared-prod.udf.bitcount_lowest_7`(days_seen_bits)),
      count(*)
    ) AS intensity
  FROM
    sample
  WHERE
    is_last_day_of_week
    AND days_since_seen < 7
  GROUP BY
    week_start,
    country_name
),
new_profile_rate AS (
  SELECT
    country_name,
    SAFE_DIVIDE(
      100 * COUNTIF(
        `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_created_profile_bits) < 7
      ), -- new profiles
      COUNTIF(`moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_seen_bits) < 7)
    ) AS new_profile_rate, -- active profiles
    week_start
  FROM
    sample
  WHERE
    is_last_day_of_week
  GROUP BY
    week_start,
    country_name
),
active_clients_weekly AS (
  SELECT
    country_name,
    client_id,
    split(app_version, '.')[offset(0)] AS major_version,
    date_sub(submission_date, INTERVAL days_since_seen DAY) AS last_day_seen,
    week_start
  FROM
    sample
  WHERE
    is_last_day_of_week
    AND days_since_seen < 7
),
latest_releases AS (
  SELECT
    MAX(SPLIT(build.target.version, '.')[OFFSET(0)]) AS latest_major_version,
    DATE(build.build.date) AS day
  FROM
    `moz-fx-data-shared-prod.telemetry.buildhub2`
  WHERE
    build.target.channel = 'release'
    AND DATE(build.build.date) >= '2018-12-01'
  GROUP BY
    day
),
active_clients_with_latest_releases AS (
  SELECT
    client_id,
    country_name,
    major_version,
    max(latest_major_version) AS latest_major_version,
    week_start
  FROM
    active_clients_weekly
  JOIN
    latest_releases
  ON
    latest_releases.day <= active_clients_weekly.last_day_seen
  WHERE
    client_id IS NOT NULL
  GROUP BY
    client_id,
    country_name,
    major_version,
    week_start
),
latest_version_ratio AS (
  SELECT
    country_name,
    SAFE_DIVIDE(COUNTIF(major_version = latest_major_version), count(*)) AS latest_version_ratio,
    week_start
  FROM
    active_clients_with_latest_releases
  GROUP BY
    country_name,
    week_start
)
SELECT
  mau.week_start,
  mau.country_name,
  mau.mau,
  daily_usage.avg_hours_usage_daily,
  intensity.intensity,
  new_profile_rate.new_profile_rate,
  latest_version_ratio.latest_version_ratio
FROM
  mau
JOIN
  daily_usage
USING
  (week_start, country_name)
JOIN
  intensity
USING
  (week_start, country_name)
JOIN
  new_profile_rate
USING
  (week_start, country_name)
JOIN
  latest_version_ratio
USING
  (week_start, country_name)
ORDER BY
  week_start,
  country_name
