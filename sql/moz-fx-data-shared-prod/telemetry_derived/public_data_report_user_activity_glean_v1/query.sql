-- Desktop and mobile MAU per country code, read on the week's Monday, as in the legacy table
WITH mau_by_code_staging AS (
  SELECT
    IFNULL(NULLIF(country, ''), '??') AS country_code,
    SUM(
      IF(app_name IN ('Firefox Desktop', 'Firefox Desktop MozillaOnline'), mau, 0)
    ) AS mau_desktop,
    SUM(
      IF(
        app_name IN ('Fenix', 'Fenix MozillaOnline', 'Firefox iOS', 'Focus Android', 'Focus iOS'),
        mau,
        0
      )
    ) AS mau_mobile
  FROM
    `moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates`
  WHERE
    submission_date = @submission_date
    AND app_name IN (
      'Firefox Desktop',
      'Firefox Desktop MozillaOnline',
      'Fenix',
      'Fenix MozillaOnline',
      'Firefox iOS',
      'Focus Android',
      'Focus iOS'
    )
  GROUP BY
    country_code
),
-- adds Worldwide, summed over codes before codes with several names are repeated
mau_with_worldwide_intermediate AS (
  SELECT
    country_code AS country_group,
    mau_desktop,
    mau_mobile
  FROM
    mau_by_code_staging
  UNION ALL
  SELECT
    'Worldwide' AS country_group,
    SUM(mau_desktop) AS mau_desktop,
    SUM(mau_mobile) AS mau_mobile
  FROM
    mau_by_code_staging
),
-- each Desktop client's full locale and country on the week's Sunday, in the 1% sample
sunday_clients_staging AS (
  SELECT
    client_id,
    IFNULL(NULLIF(country, ''), '??') AS country_code,
    NULLIF(locale, '') AS locale
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_clients_last_seen`
  WHERE
    submission_date = DATE_ADD(@submission_date, INTERVAL 6 DAY)
    AND sample_id = 1
),
-- Desktop clients active in the week ending that Sunday, in the 1% sample
sunday_wau_staging AS (
  SELECT
    client_id
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE
    submission_date = DATE_ADD(@submission_date, INTERVAL 6 DAY)
    AND sample_id = 1
    AND is_wau
    AND app_name IN ('Firefox Desktop', 'Firefox Desktop MozillaOnline')
),
-- weekly active clients with a locale, each under its country and under Worldwide
locale_clients_intermediate AS (
  SELECT
    sunday_clients_staging.client_id,
    country_group,
    sunday_clients_staging.locale
  FROM
    sunday_clients_staging
  JOIN
    sunday_wau_staging
    ON sunday_wau_staging.client_id = sunday_clients_staging.client_id
  CROSS JOIN
    UNNEST([sunday_clients_staging.country_code, 'Worldwide']) AS country_group
  WHERE
    sunday_clients_staging.locale IS NOT NULL
),
-- clients per country group and locale
locale_counts_intermediate AS (
  SELECT
    country_group,
    locale,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    locale_clients_intermediate
  GROUP BY
    country_group,
    locale
),
-- clients with a locale per country group, the denominator for each share
locale_totals_intermediate AS (
  SELECT
    country_group,
    COUNT(DISTINCT client_id) AS clients_with_locale
  FROM
    locale_clients_intermediate
  GROUP BY
    country_group
),
-- the top five locales per country group, ties ordered by locale
top_locales_intermediate AS (
  SELECT
    country_group,
    ARRAY_AGG(
      STRUCT(locale, client_count / clients_with_locale AS ratio)
      ORDER BY
        client_count DESC,
        locale
      LIMIT
        5
    ) AS top_locales
  FROM
    locale_counts_intermediate
  JOIN
    locale_totals_intermediate
    USING (country_group)
  GROUP BY
    country_group
),
-- country codes and their names; a code can have several names
countries_staging AS (
  SELECT
    code,
    name
  FROM
    `moz-fx-data-shared-prod.static.country_names_v1`
)
-- one row per country name, plus Worldwide; a country with no locale clients gets an empty list
SELECT
  @submission_date AS submission_date,
  COALESCE(countries_staging.name, mau_with_worldwide_intermediate.country_group) AS country_name,
  mau_with_worldwide_intermediate.mau_desktop + mau_with_worldwide_intermediate.mau_mobile AS mau,
  IFNULL(
    top_locales_intermediate.top_locales,
    ARRAY<STRUCT<locale STRING, ratio FLOAT64>>[]
  ) AS top_locales,
  mau_with_worldwide_intermediate.mau_desktop,
  mau_with_worldwide_intermediate.mau_mobile
FROM
  mau_with_worldwide_intermediate
LEFT JOIN
  top_locales_intermediate
  USING (country_group)
LEFT JOIN
  countries_staging
  ON countries_staging.code = mau_with_worldwide_intermediate.country_group
