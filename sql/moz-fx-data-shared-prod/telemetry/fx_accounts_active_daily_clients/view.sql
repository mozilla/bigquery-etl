CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fx_accounts_active_daily_clients`
AS
SELECT
  *,
  CASE
    WHEN normalized_os = 'Windows'
      AND normalized_os_version = '10.0'
      AND windows_build_number < 22000
      THEN 'Windows 10'
    WHEN normalized_os = 'Windows'
      AND normalized_os_version = '10.0'
      AND windows_build_number >= 22000
      THEN 'Windows 11'
    ELSE normalized_os || ' ' || normalized_os_version
  END AS os_version_build
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_active_daily_clients_v1`
