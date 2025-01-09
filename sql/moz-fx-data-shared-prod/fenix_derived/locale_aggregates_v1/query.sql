-- Query for fenix_derived.locale_aggregates_v1
WITH metrics AS (
    -- Metrics ping may arrive in the same or next day as the baseline ping.
  SELECT
    client_id,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(is_default_browser IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS is_default_browser
  FROM
    `moz-fx-data-shared-prod.fenix.metrics_clients_last_seen`
  WHERE
    DATE(submission_date)
    BETWEEN @submission_date
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  GROUP BY
    client_id
),
baseline AS (
  SELECT
    client_id,
    submission_date,
    app_name,
    app_display_version,
    normalized_channel,
    country,
    city,
    locale,
    normalized_os,
    normalized_os_version,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau
  FROM
    `moz-fx-data-shared-prod.fenix.active_users`
  WHERE
    submission_date = @submission_date
),
unioned AS (
  SELECT
    baseline.client_id,
    baseline.submission_date,
    baseline.app_name,
    baseline.app_display_version AS app_version,
    baseline.normalized_channel AS channel,
    IFNULL(baseline.country, '??') country,
    baseline.city,
    baseline.locale,
    baseline.normalized_os AS os,
    baseline.normalized_os_version AS os_version,
    metrics.is_default_browser,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau
  FROM
    baseline
  LEFT JOIN
    metrics
    ON baseline.client_id = metrics.client_id
    AND baseline.normalized_channel IS NOT DISTINCT FROM metrics.normalized_channel
)
SELECT
  unioned.* EXCEPT (
    client_id,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau
  ),
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau
FROM
  unioned
GROUP BY
  submission_date,
  app_name,
  app_version,
  channel,
  country,
  city,
  locale,
  os,
  os_version,
  is_default_browser
