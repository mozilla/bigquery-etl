CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.newtab_clients_daily_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.newtab_clients_daily_aggregates_v1`
