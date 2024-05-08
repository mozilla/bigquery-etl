CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.baseline_clients_last_seen_extended_activity`
AS
-- This view is being added temporarily until issues preventing
-- https://github.com/mozilla/bigquery-etl/pull/5434
-- from merging have been resolved.
SELECT
  last_seen.*,
  CASE
    WHEN LOWER(isp) = 'browserstack'
      THEN CONCAT("Fenix", ' ', isp)
    WHEN LOWER(clients.distribution_id) = 'mozillaonline'
      THEN CONCAT("Fenix", ' ', clients.distribution_id)
    ELSE "Fenix"
  END AS app_name,
  -- Activity fields to support metrics built on top of activity
  CASE
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 1
      AND 6
      THEN 'infrequent_user'
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 7
      AND 13
      THEN 'casual_user'
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 14
      AND 20
      THEN 'regular_user'
    WHEN BIT_COUNT(days_active_bits) >= 21
      THEN 'core_user'
    ELSE 'other'
  END AS activity_segment,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  -- Metrics based on pings sent
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  (
    LOWER(IFNULL(isp, "")) <> "browserstack"
    AND LOWER(IFNULL(clients.distribution_id, "")) <> "mozillaonline"
  ) AS is_mobile,  -- Indicates which records should be used for mobile KPI metric calculations.
  FALSE AS is_desktop,
FROM
  `moz-fx-data-shared-prod.fenix.baseline_clients_last_seen` AS last_seen
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.firefox_android_clients` AS clients
  USING (client_id, first_seen_date)
