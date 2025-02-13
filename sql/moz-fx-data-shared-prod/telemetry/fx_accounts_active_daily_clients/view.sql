CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fxa_accounts_active_daily_clients`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.fx_accounts_active_daily_clients_v1`
