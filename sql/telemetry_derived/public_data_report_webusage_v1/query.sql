WITH countries AS (
  SELECT
    code,
    name
  FROM
    `moz-fx-data-shared-prod.static.country_names_v1`
),
sample AS (
  SELECT
    DATE_TRUNC(submission_date, WEEK(MONDAY)) AS week_start,
    EXTRACT(DAYOFWEEK FROM submission_date) = 1 AS is_last_day_of_week,
    COALESCE(cn.name, country_group) AS country_name,
    client_id,
    locale,
    addons.is_system,
    addons.foreign_install,
    addons.addon_id,
    addons.name AS addon_name
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_last_seen`,
    UNNEST(
      IF(
        ARRAY_LENGTH(active_addons) > 0,
        active_addons,
        -- include a null addon if there were none (either null or an empty list)
        [active_addons[SAFE_OFFSET(0)]]
      )
    ) AS addons,
    UNNEST([country, 'Worldwide']) AS country_group
  LEFT JOIN
    countries AS cn
  ON
    cn.code = country_group
  WHERE
    submission_date = DATE_ADD(@submission_date, INTERVAL 6 DAY)
    AND days_since_seen < 7
    AND EXTRACT(
      DAYOFWEEK
      FROM
        submission_date
    ) = 1 -- last day of week
    AND COALESCE(cn.name, country_group) IN (
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
),
wau AS (
  SELECT
    week_start,
    country_name,
    count(DISTINCT client_id) AS total_users
  FROM
    sample
  GROUP BY
    week_start,
    country_name
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
    sample
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
    user_count / total_users AS ratio
  FROM
    addon_counts
  JOIN
    wau
  USING
    (week_start, country_name)
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
    sample
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
    user_count / total_users AS ratio
  FROM
    locale_counts
  JOIN
    wau
  USING
    (week_start, country_name)
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
  top_addons.week_start,
  top_addons.country_name,
  top_addons.top_addons,
  has_addon.has_addon_ratio,
  top_locales.top_locales
FROM
  top_addons
JOIN
  top_locales
USING
  (week_start, country_name)
JOIN
  has_addon
USING
  (week_start, country_name)
