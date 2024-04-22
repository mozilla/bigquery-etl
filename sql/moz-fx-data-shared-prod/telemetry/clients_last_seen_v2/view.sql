CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_last_seen_v2`
AS
SELECT
  cls.* EXCEPT (app_name),
  CASE
    WHEN cls.isp_name = 'BrowserStack'
      THEN CONCAT(cls.app_name, ' ', cls.isp_name)
    WHEN distribution_id = 'MozillaOnline'
      THEN CONCAT(cls.app_name, ' ', cls.distribution_id)
    ELSE cls.app_name
  END AS app_name,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) < 28, FALSE) AS is_mau,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_active_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(cls.days_seen_bits) < 28, FALSE) AS is_monthly_user,
  IF(
    LOWER(IFNULL(cls.isp_name, '')) <> "browserstack"
    AND LOWER(cls.distribution_id) <> "mozillaonline",
    TRUE,
    FALSE
  ) AS is_desktop
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v2` cls
