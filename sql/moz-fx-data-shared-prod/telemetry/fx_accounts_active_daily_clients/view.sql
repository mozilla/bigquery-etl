CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_accounts_active_daily_clients`
AS
SELECT
  fxaadc.*,
  bau.is_dau,
  bau.is_wau,
  bau.is_mau,
  bau.is_new_profile,
  bau.is_default_browser,
  bau.activity_segment
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_active_daily_clients_v1` fxaadc
LEFT OUTER JOIN
  `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users` bau
  ON fxaadc.client_id = bau.client_id
  AND fxaadc.submission_date = bau.submission_date
