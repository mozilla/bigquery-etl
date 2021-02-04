CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_daily_histogram_aggregates`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_daily_histogram_aggregates_v1`
