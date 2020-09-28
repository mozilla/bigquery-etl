-- We "compress" by allowing only the exact dimensions values that
-- are selectable in GUD to be broken out in the aggregates.
WITH compressed AS (
  SELECT
    submission_date,
    usage,
    id_bucket,
    metrics,
    IF(
      country IN (
        'US',
        'DE',
        'GB',
        'FR',
        'BR',
        'RU',
        'PL',
        'CN',
        'IN',
        'IT',
        'CA',
        'ES',
        'ID',
        'KE',
        'JP'
      ),
      country,
      NULL
    ) AS country,
    substr(locale, 0, 2) AS locale,
    IF(os IN ('Windows_NT', 'Darwin', 'Linux'), os, 'Other') AS os,
    channel,
    attributed,
  FROM
    smoot_usage_nondesktop_v2
)
SELECT
  submission_date,
  usage,
  id_bucket,
  country,
  locale,
  os,
  channel,
  attributed,
  STRUCT(
    STRUCT(
      sum(metrics.day_0.dau) AS dau,
      sum(metrics.day_0.wau) AS wau,
      sum(metrics.day_0.mau) AS mau,
      sum(metrics.day_0.active_days_in_week) AS active_days_in_week
    ) AS day_0,
    STRUCT(sum(metrics.day_6.new_profiles) AS new_profiles) AS day_6,
    STRUCT(
      sum(metrics.day_13.new_profiles) AS new_profiles,
      sum(metrics.day_13.active_in_week_0) AS active_in_week_0,
      sum(metrics.day_13.active_in_week_1) AS active_in_week_1,
      sum(metrics.day_13.active_in_weeks_0_and_1) AS active_in_weeks_0_and_1,
      sum(metrics.day_13.new_profile_active_in_week_0) AS new_profile_active_in_week_0,
      sum(metrics.day_13.new_profile_active_in_week_1) AS new_profile_active_in_week_1,
      sum(metrics.day_13.new_profile_active_in_weeks_0_and_1) AS new_profile_active_in_weeks_0_and_1
    ) AS day_13
  ) AS metrics
FROM
  compressed
WHERE
  (@submission_date IS NULL OR @submission_date = submission_date)
GROUP BY
  submission_date,
  usage,
  id_bucket,
  country,
  locale,
  os,
  channel,
  attributed
