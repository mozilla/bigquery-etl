CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.suggest_clients_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.suggest_clients_daily_v1`
