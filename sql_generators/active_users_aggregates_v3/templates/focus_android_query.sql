--- Query generated via sql_generators.active_users.
WITH baseline AS (
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    days_created_profile_bits,
    os AS normalized_os,
    osversion AS normalized_os_version,
    locale,
    city,
    country,
    metadata_app_version AS app_display_version,
    device AS device_model,
    first_seen_date,
    submission_date = first_seen_date AS is_new_profile,
    distribution_id,
    CAST(NULL AS string) AS isp,
    'Focus Android Legacy' AS app_name,
    CAST(NULL AS STRING) AS segment,
    CAST(NULL AS BOOLEAN) AS is_daily_user,
    CAST(NULL AS BOOLEAN) AS is_weekly_user,
    CAST(NULL AS BOOLEAN) AS is_monthly_user,
    CAST(NULL AS BOOLEAN) AS is_dau,
    CAST(NULL AS BOOLEAN) AS is_wau,
    CAST(NULL AS BOOLEAN) AS is_mau
  FROM
    `{{ project_id }}.telemetry.core_clients_last_seen`
  WHERE
    submission_date = @submission_date
    AND app_name = 'Focus'
    AND os = 'Android'
  UNION ALL
  SELECT
    submission_date,
    normalized_channel,
    client_id,
    days_created_profile_bits,
    normalized_os,
    normalized_os_version,
    locale,
    city,
    country,
    app_display_version,
    device_model,
    first_seen_date,
    submission_date = first_seen_date AS is_new_profile,
    CAST(NULL AS string) AS distribution_id,
    isp,
    app_name,
    activity_segment AS segment,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau
  FROM
    `{{ project_id }}.{{ app_name }}.active_users`
  WHERE
    submission_date = @submission_date
),
metrics AS (
  -- Metrics ping can arrive either in the same or next day as the baseline ping.
  SELECT
    client_id,
    ARRAY_AGG(normalized_channel IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS normalized_channel,
    ARRAY_AGG(uri_count IGNORE NULLS ORDER BY submission_date ASC)[SAFE_OFFSET(0)] AS uri_count,
    ARRAY_AGG(is_default_browser IGNORE NULLS ORDER BY submission_date ASC)[
      SAFE_OFFSET(0)
    ] AS is_default_browser
  FROM
    `{{ project_id }}.{{ app_name }}.metrics_clients_last_seen`
  WHERE
    DATE(submission_date)
    BETWEEN @submission_date
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
  GROUP BY
    client_id
),
unioned AS (
  SELECT
    baseline.client_id,
    baseline.segment,
    baseline.app_name,
    baseline.app_display_version AS app_version,
    baseline.normalized_channel,
    IFNULL(baseline.country, '??') country,
    baseline.city,
    baseline.days_created_profile_bits,
    baseline.device_model,
    baseline.isp,
    baseline.is_new_profile,
    baseline.locale,
    baseline.first_seen_date,
    baseline.normalized_os,
    baseline.normalized_os_version,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(baseline.normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER),
      0
    ) AS os_version_patch,
    baseline.submission_date,
    metrics.uri_count,
    metrics.is_default_browser,
    baseline.distribution_id,
    CAST(NULL AS string) AS attribution_content,
    CAST(NULL AS string) AS attribution_source,
    CAST(NULL AS string) AS attribution_medium,
    CAST(NULL AS string) AS attribution_campaign,
    CAST(NULL AS string) AS attribution_experiment,
    CAST(NULL AS string) AS attribution_variation,
    CAST(NULL AS FLOAT64) AS active_hours_sum,
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
),
todays_metrics AS (
  SELECT
    segment,
    app_version,
    attribution_medium,
    attribution_source,
    attribution_medium IS NOT NULL
    OR attribution_source IS NOT NULL AS attributed,
    city,
    country,
    distribution_id,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
    is_default_browser,
    COALESCE(REGEXP_EXTRACT(locale, r'^(.+?)-'), locale, NULL) AS locale,
    app_name AS app_name,
    normalized_channel AS channel,
    normalized_os AS os,
    normalized_os_version AS os_version,
    os_version_major,
    os_version_minor,
    submission_date,
    client_id,
    uri_count,
    active_hours_sum,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau,
    CAST(NULL AS STRING) AS adjust_network,
    CAST(NULL AS STRING) AS install_source
  FROM
    unioned
)
SELECT
  todays_metrics.* EXCEPT (
    client_id,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau,
    uri_count,
    active_hours_sum
  ),
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
  SUM(uri_count) AS uri_count,
  SUM(active_hours_sum) AS active_hours,
FROM
  todays_metrics
GROUP BY
  app_version,
  attribution_medium,
  attribution_source,
  attributed,
  city,
  country,
  distribution_id,
  first_seen_year,
  is_default_browser,
  locale,
  app_name,
  channel,
  os,
  os_version,
  os_version_major,
  os_version_minor,
  submission_date,
  segment,
  adjust_network,
  install_source
