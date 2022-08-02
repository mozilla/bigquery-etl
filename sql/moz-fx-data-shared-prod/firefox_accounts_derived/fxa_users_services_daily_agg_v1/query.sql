WITH BASE AS (
  SELECT
    submission_date,
    service,
    country,
    LANGUAGE,
    mozfun.norm.truncate_version(app_version, "minor") AS app_version,
    os_name,
    os_version,
    user_id,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_daily_v1`
  WHERE
    submission_date = DATE(@submission_date)
),
desktop_and_mobile_users AS (
  SELECT
    submission_date,
    "desktop_and_mobile" AS service,
    country,
    LANGUAGE,
    app_version,
    os_name,
    os_version,
    COUNT(DISTINCT user_id) AS service_user_count,
  FROM
    base
  WHERE
    service IN ('firefox-desktop', 'fenix', 'firefox-ios')
  GROUP BY
    submission_date,
    service,
    country,
    LANGUAGE,
    app_version,
    os_name,
    os_version
),
service_users AS (
  SELECT
    submission_date,
    service,
    country,
    LANGUAGE,
    app_version,
    os_name,
    os_version,
    COUNT(DISTINCT user_id) AS service_user_count,
  FROM
    base
  GROUP BY
    submission_date,
    service,
    country,
    LANGUAGE,
    app_version,
    os_name,
    os_version
),
final AS (
  SELECT
    *
  FROM
    desktop_and_mobile_users
  UNION ALL
  SELECT
    *
  FROM
    service_users
)
SELECT
  *
FROM
  final
