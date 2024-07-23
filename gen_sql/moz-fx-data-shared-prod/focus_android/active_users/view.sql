-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.focus_android.active_users`
AS
SELECT
  * EXCEPT (isp),
  CASE
    WHEN LOWER(isp) = "browserstack"
      THEN CONCAT("Focus Android", " ", isp)
    WHEN LOWER(distribution_id) = "mozillaonline"
      THEN CONCAT("Focus Android", " ", distribution_id)
    ELSE "Focus Android"
  END AS app_name,
  -- Activity fields to support metrics built on top of activity
  CASE
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 1
      AND 6
      THEN "infrequent_user"
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 7
      AND 13
      THEN "casual_user"
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 14
      AND 20
      THEN "regular_user"
    WHEN BIT_COUNT(days_active_bits) >= 21
      THEN "core_user"
    ELSE "other"
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
    AND LOWER(IFNULL(distribution_id, "")) <> "mozillaonline"
  ) AS is_mobile,
  -- Adding isp at the end because it's in different column index in baseline table for some products.
  -- This is to make sure downstream union works as intended.
  isp,
FROM
  `moz-fx-data-shared-prod.focus_android.baseline_clients_last_seen`
