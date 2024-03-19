CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
AS
SELECT
    submission_date,
    client_id,
    CASE
        WHEN isp_name = 'BrowserStack' THEN CONCAT(app_name, ' BrowserStack')
        WHEN distribution_id = 'Mozilla Online' THEN CONCAT(app_name, ' Mozilla Online')
        ELSE app_name END
    AS app_name,
    days_seen_bits,
    days_active_bits,
    CAST(mozfun.bits28.days_since_seen(days_active_bits)=0 AS BOOLEAN) AS is_dau,
    CAST(mozfun.bits28.days_since_seen(days_active_bits)<=7 AS BOOLEAN) AS is_wau,
    CAST(mozfun.bits28.days_since_seen(days_active_bits) AS BOOLEAN) AS is_mau,
    CAST(mozfun.bits28.days_since_seen(days_seen_bits)=0 AS BOOLEAN) AS is_daily_user,
    CAST(mozfun.bits28.days_since_seen(days_active_bits)<=7 AS BOOLEAN) AS is_weekly_user,
    CAST(mozfun.bits28.days_since_seen(days_seen_bits) AS BOOLEAN) AS is_monthly_user,
    CAST(1 AS BOOLEAN) AS is_desktop,
    CAST(0 AS BOOLEAN) AS is_mobile
FROM `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v2`
