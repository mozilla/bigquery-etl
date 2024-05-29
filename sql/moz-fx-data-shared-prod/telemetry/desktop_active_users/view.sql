CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.desktop_active_users`
AS
SELECT
  submission_date,
  client_id,
  sample_id,
  days_seen_bits,
  days_active_bits,
  CASE
    WHEN isp_name = 'BrowserStack'
      THEN CONCAT(app_name, ' ', isp_name)
    WHEN distribution_id = 'MozillaOnline'
      THEN CONCAT(app_name, ' ', distribution_id)
    ELSE app_name
  END AS app_name,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  IF(
    LOWER(IFNULL(isp_name, '')) <> "browserstack"
    AND LOWER(IFNULL(distribution_id, '')) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v2`
