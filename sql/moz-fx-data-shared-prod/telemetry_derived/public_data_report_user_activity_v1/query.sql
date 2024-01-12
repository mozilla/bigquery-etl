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
    app_version,
    locale,
    active_addons
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`,
    UNNEST([country, 'Worldwide']) AS country_group
  LEFT JOIN
    countries AS cn
    ON cn.code = country_group
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
    AND sample_id = 1
),
sample_addons AS (
  SELECT
    week_start,
    is_last_day_of_week,
    country_name,
    client_id,
    locale,
    addons.is_system,
    addons.foreign_install,
    addons.addon_id,
    addons.name AS addon_name
  FROM
    sample,
    UNNEST(
      IF(
        ARRAY_LENGTH(active_addons) > 0,
        active_addons,
            -- include a null addon if there were none (either null or an empty list)
        [active_addons[SAFE_OFFSET(0)]]
      )
    ) AS addons
  WHERE
    days_since_seen < 7
    AND is_last_day_of_week
),
mau_wau AS (
  SELECT
    week_start,
    country_name,
    COUNT(DISTINCT IF(days_since_seen < 28, client_id, NULL)) AS mau,
    COUNT(DISTINCT IF(days_since_seen < 7, client_id, NULL)) AS wau
  FROM
    sample
  WHERE
    is_last_day_of_week
  GROUP BY
    week_start,
    country_name
),
avg_daily_usage_by_user AS (
  SELECT
    client_id,
    country_name,
    AVG(subsession_hours_sum) AS avg_hours_usage_daily_per_user,
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
    AVG(avg_hours_usage_daily_per_user) AS avg_hours_usage_daily,
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
      COUNT(*)
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
      COUNTIF(
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
    `mozfun.norm.truncate_version`(app_version, "major") AS major_version,
    DATE_SUB(submission_date, INTERVAL days_since_seen DAY) AS last_day_seen,
    week_start
  FROM
    sample
  WHERE
    is_last_day_of_week
    AND days_since_seen < 7
),
latest_releases AS (
  SELECT
    MAX(`mozfun.norm.truncate_version`(build.target.version, "major")) AS latest_major_version,
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
    MAX(latest_major_version) AS latest_major_version,
    week_start
  FROM
    active_clients_weekly
  JOIN
    latest_releases
    ON latest_releases.day <= active_clients_weekly.last_day_seen
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
    SAFE_DIVIDE(COUNTIF(major_version = latest_major_version), COUNT(*)) AS latest_version_ratio,
    week_start
  FROM
    active_clients_with_latest_releases
  GROUP BY
    country_name,
    week_start
),
addon_counts AS (
  SELECT
    week_start,
    country_name,
    addon_id,
    addon_name,
    COUNT(
      DISTINCT IF(
        is_system = FALSE
        AND foreign_install = FALSE
        AND addon_id NOT LIKE '%@mozilla%'
        AND addon_id NOT LIKE '%@shield.mozilla%'
        AND addon_ID NOT LIKE '%@unified-urlbar-shield-study-%'
        AND addon_id NOT LIKE '%@testpilot-addon%'
        AND addon_id NOT LIKE '%@testpilot-addon%'
        AND addon_id NOT LIKE '%@activity-streams%'
        AND addon_id NOT LIKE '%support@laserlike.com%'
        AND addon_id NOT LIKE '%testpilot@cliqz.com%'
        AND addon_id NOT LIKE '%@testpilot-containers%'
        AND addon_id NOT LIKE '%@sloth%'
        AND addon_id NOT LIKE '%@min-vid%'
        AND addon_id NOT LIKE '%jid1-NeEaf3sAHdKHPA@jetpack%',
        client_id,
        NULL
      )
    ) AS user_count
  FROM
    sample_addons
  GROUP BY
    week_start,
    country_name,
    addon_id,
    addon_name
),
addon_ratios AS (
  SELECT
    week_start,
    country_name,
    addon_name,
    user_count / wau AS ratio
  FROM
    addon_counts
  JOIN
    mau_wau
    USING (week_start, country_name)
),
top_addons AS (
  SELECT
    week_start,
    country_name,
    ARRAY_AGG(STRUCT(addon_name, ratio) ORDER BY ratio DESC LIMIT 10) AS top_addons
  FROM
    addon_ratios
  GROUP BY
    week_start,
    country_name
),
has_addon AS (
  SELECT
    week_start,
    country_name,
    COUNT(
      DISTINCT IF(
        is_system = FALSE
        AND foreign_install = FALSE
        AND addon_id NOT LIKE '%@mozilla%'
        AND addon_id NOT LIKE '%@shield.mozilla%'
        AND addon_ID NOT LIKE '%@unified-urlbar-shield-study-%'
        AND addon_id NOT LIKE '%@testpilot-addon%'
        AND addon_id NOT LIKE '%@testpilot-addon%'
        AND addon_id NOT LIKE '%@activity-streams%'
        AND addon_id NOT LIKE '%support@laserlike.com%'
        AND addon_id NOT LIKE '%testpilot@cliqz.com%'
        AND addon_id NOT LIKE '%@testpilot-containers%'
        AND addon_id NOT LIKE '%@sloth%'
        AND addon_id NOT LIKE '%@min-vid%'
        AND addon_id NOT LIKE '%jid1-NeEaf3sAHdKHPA@jetpack%',
        client_id,
        NULL
      )
    ) / COUNT(DISTINCT client_id) AS has_addon_ratio
  FROM
    sample_addons
  GROUP BY
    week_start,
    country_name
),
locale_counts AS (
  SELECT
    week_start,
    country_name,
    locale,
    COUNT(DISTINCT client_id) AS user_count
  FROM
    sample
  WHERE
    days_since_seen < 7
    AND is_last_day_of_week
  GROUP BY
    week_start,
    country_name,
    locale
),
locale_ratios AS (
  SELECT
    week_start,
    country_name,
    locale,
    user_count / wau AS ratio
  FROM
    locale_counts
  JOIN
    mau_wau
    USING (week_start, country_name)
),
top_locales AS (
  SELECT
    week_start,
    country_name,
    ARRAY_AGG(STRUCT(locale, ratio) ORDER BY ratio DESC LIMIT 5) AS top_locales
  FROM
    locale_ratios
  GROUP BY
    week_start,
    country_name
)
SELECT
  mau_wau.week_start AS submission_date,
  mau_wau.country_name,
  mau_wau.mau,
  daily_usage.avg_hours_usage_daily,
  intensity.intensity,
  new_profile_rate.new_profile_rate,
  latest_version_ratio.latest_version_ratio,
  top_addons.top_addons,
  has_addon.has_addon_ratio,
  top_locales.top_locales
FROM
  mau_wau
JOIN
  daily_usage
  USING (week_start, country_name)
JOIN
  intensity
  USING (week_start, country_name)
JOIN
  new_profile_rate
  USING (week_start, country_name)
JOIN
  latest_version_ratio
  USING (week_start, country_name)
JOIN
  top_addons
  USING (week_start, country_name)
JOIN
  top_locales
  USING (week_start, country_name)
JOIN
  has_addon
  USING (week_start, country_name)
